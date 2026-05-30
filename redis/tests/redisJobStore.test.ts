import { describe, expect, test } from 'bun:test';
import { Type as t } from '@sinclair/typebox';
import { defineJobs } from '@absolutejs/queue';
import { createRedisJobStore, type RedisCommandClient } from '../src/index';

/**
 * Mock Redis client. In-process state shaped like the keys the adapter
 * uses (HASH per job, ZSET for due/claimed, STRING for idempotency).
 * Includes a small Lua interpreter just for the two scripts the
 * adapter ships (CLAIM_LUA + REAP_LUA) so the atomic-claim contract is
 * actually exercised. Behavior is OK to evolve as the adapter does.
 */
const makeMockRedis = () => {
	const hashes = new Map<string, Map<string, string>>();
	const zsets = new Map<string, Map<string, number>>();
	const strings = new Map<string, string>();
	const sets = new Map<string, Set<string>>();

	const getHash = (key: string): Map<string, string> => {
		let m = hashes.get(key);
		if (m === undefined) {
			m = new Map();
			hashes.set(key, m);
		}
		return m;
	};
	const getZset = (key: string): Map<string, number> => {
		let m = zsets.get(key);
		if (m === undefined) {
			m = new Map();
			zsets.set(key, m);
		}
		return m;
	};

	const runClaim = (
		keys: string[],
		args: string[]
	): Array<Array<string>> => {
		const [dueKey, claimedKey, jobKeyPrefix] = keys;
		const now = Number(args[0]);
		const limit = Number(args[1]);
		const workerId = args[2]!;
		const due = getZset(dueKey!);
		const claimed = getZset(claimedKey!);
		const eligible = [...due.entries()]
			.filter(([, score]) => score <= now)
			.sort((a, b) => a[1] - b[1])
			.slice(0, limit);
		const result: Array<Array<string>> = [];
		for (const [id] of eligible) {
			due.delete(id);
			claimed.set(id, now); // Mock: stores lockedAt; real Lua uses lockedAt + leaseMs (which the adapter passes as 0 for tests of claimDue, so equivalent).
			const hash = getHash(`${jobKeyPrefix}${id}`);
			hash.set('status', 'claimed');
			hash.set('lockedAt', String(now));
			hash.set('lockedBy', workerId);
			hash.set('updatedAt', String(now));
			const flat: string[] = [];
			for (const [field, value] of hash.entries()) {
				flat.push(field, value);
			}
			result.push(flat);
		}
		return result;
	};

	const runReap = (keys: string[], args: string[]): number => {
		const [claimedKey, dueKey, jobKeyPrefix] = keys;
		const now = Number(args[0]);
		const claimed = getZset(claimedKey!);
		const due = getZset(dueKey!);
		const expired = [...claimed.entries()].filter(([, s]) => s <= now);
		for (const [id] of expired) {
			claimed.delete(id);
			const hash = getHash(`${jobKeyPrefix}${id}`);
			const runAt = Number(hash.get('runAt') ?? now);
			due.set(id, runAt);
			hash.delete('lockedAt');
			hash.delete('lockedBy');
			hash.set('status', 'pending');
			hash.set('updatedAt', String(now));
		}
		return expired.length;
	};

	const client: RedisCommandClient = {
		del: async (...keys) => {
			for (const k of keys) {
				hashes.delete(k);
				zsets.delete(k);
				strings.delete(k);
				sets.delete(k);
			}
		},
		eval: async (script, keys, args) => {
			// Distinguish CLAIM vs REAP by their unique markers: CLAIM
			// sets status='claimed' + writes lockedBy; REAP sets
			// status='pending' and HDELs the lock fields.
			if (script.includes("'lockedBy', workerId")) {
				return runClaim(keys, args);
			}
			if (script.includes("'lockedAt', 'lockedBy'") && script.includes("'pending'")) {
				return runReap(keys, args);
			}
			return null;
		},
		get: async (key) => strings.get(key) ?? null,
		hdel: async (key, ...names) => {
			const h = hashes.get(key);
			if (h === undefined) return;
			for (const n of names) h.delete(n);
		},
		hgetall: async (key) => {
			const h = hashes.get(key);
			if (h === undefined) return null;
			return Object.fromEntries(h.entries());
		},
		hset: async (key, fieldsObj) => {
			const h = getHash(key);
			for (const [k, v] of Object.entries(fieldsObj)) h.set(k, v);
		},
		sadd: async (key, ...members) => {
			let s = sets.get(key);
			if (s === undefined) {
				s = new Set();
				sets.set(key, s);
			}
			for (const m of members) s.add(m);
		},
		scard: async (key) => sets.get(key)?.size ?? 0,
		set: async (key, value, mode) => {
			if (mode === 'NX' && strings.has(key)) return null;
			strings.set(key, value);
			return 'OK';
		},
		smembers: async (key) => [...(sets.get(key) ?? new Set())],
		srem: async (key, ...members) => {
			const s = sets.get(key);
			if (s === undefined) return;
			for (const m of members) s.delete(m);
		},
		zadd: async (key, score, member) => {
			getZset(key).set(member, score);
		},
		zcard: async (key) => zsets.get(key)?.size ?? 0,
		zrangebyscore: async (key, min, max) => {
			const z = zsets.get(key);
			if (z === undefined) return [];
			const minN = min === '-inf' ? -Infinity : Number(min);
			const maxN = max === '+inf' ? Infinity : Number(max);
			return [...z.entries()]
				.filter(([, score]) => score >= minN && score <= maxN)
				.sort((a, b) => a[1] - b[1])
				.map(([id]) => id);
		},
		zrem: async (key, ...members) => {
			const z = zsets.get(key);
			if (z === undefined) return;
			for (const m of members) z.delete(m);
		}
	};
	return { client, hashes, sets, strings, zsets };
};

const jobs = defineJobs({
	'email.send': t.Object({ to: t.String(), subject: t.String() }),
	'math.add': t.Object({ a: t.Number(), b: t.Number() })
});

describe('createRedisJobStore — enqueue + claim + complete', () => {
	test('enqueue stores the job + adds to due ZSET', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore<{
			'email.send': { to: string; subject: string };
			'math.add': { a: number; b: number };
		}>({ client: mock.client });
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { a: 1, b: 2 }
		});
		expect(id).toBeDefined();
		const due = mock.zsets.get('absolutejs:queue:due')!;
		expect(due.has(id)).toBe(true);
		const job = await store.get!(id);
		expect(job!.kind).toBe('math.add');
		expect(job!.payload).toEqual({ a: 1, b: 2 });
		expect(job!.status).toBe('pending');
	});

	test('claimDue moves due jobs to claimed and returns them', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore<{
			'math.add': { a: number; b: number };
		}>({ client: mock.client });
		await store.enqueue({ kind: 'math.add', payload: { a: 1, b: 2 } });
		await store.enqueue({ kind: 'math.add', payload: { a: 3, b: 4 } });
		const claimed = await store.claimDue({
			limit: 10,
			now: Date.now(),
			workerId: 'worker-1'
		});
		expect(claimed).toHaveLength(2);
		expect(claimed[0]!.status).toBe('claimed');
		expect(claimed[0]!.lockedBy).toBe('worker-1');
		expect(mock.zsets.get('absolutejs:queue:due')!.size).toBe(0);
		expect(mock.zsets.get('absolutejs:queue:claimed')!.size).toBe(2);
	});

	test('claimDue respects limit', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		for (let i = 0; i < 5; i++) {
			await store.enqueue({ kind: 'math.add', payload: { a: i, b: 0 } });
		}
		const claimed = await store.claimDue({
			limit: 3,
			now: Date.now(),
			workerId: 'w'
		});
		expect(claimed).toHaveLength(3);
		expect(mock.zsets.get('absolutejs:queue:claimed')!.size).toBe(3);
		expect(mock.zsets.get('absolutejs:queue:due')!.size).toBe(2);
	});

	test('future-runAt jobs not claimed until time arrives', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		const now = 1_000_000;
		await store.enqueue({
			kind: 'math.add',
			payload: { a: 1, b: 2 },
			runAt: now + 10_000
		});
		// Try to claim before the runAt — no result.
		expect(
			(await store.claimDue({ limit: 10, now, workerId: 'w' })).length
		).toBe(0);
		// At runAt — claimed.
		expect(
			(await store.claimDue({ limit: 10, now: now + 10_000, workerId: 'w' }))
				.length
		).toBe(1);
	});

	test('complete removes from claimed + due, marks done', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { a: 1, b: 2 }
		});
		await store.claimDue({ limit: 1, now: Date.now(), workerId: 'w' });
		await store.complete(id);
		const job = await store.get!(id);
		expect(job!.status).toBe('done');
		expect(mock.zsets.get('absolutejs:queue:claimed')!.size).toBe(0);
		expect(mock.zsets.get('absolutejs:queue:due')!.size).toBe(0);
	});
});

describe('fail + retry + dead-letter', () => {
	test('fail with retryAt restores to due at the new score', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { a: 1, b: 2 }
		});
		await store.claimDue({ limit: 1, now: Date.now(), workerId: 'w' });
		await store.fail(id, { error: 'transient', retryAt: 2_000_000 });
		const job = await store.get!(id);
		expect(job!.status).toBe('pending');
		expect(job!.attempts).toBe(1);
		expect(job!.runAt).toBe(2_000_000);
		expect(job!.lastError).toBe('transient');
		expect(mock.zsets.get('absolutejs:queue:claimed')!.size).toBe(0);
		expect(mock.zsets.get('absolutejs:queue:due')!.get(id)).toBe(
			2_000_000
		);
	});

	test('fail with dead=true marks dead, no re-queue', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { a: 1, b: 2 }
		});
		await store.claimDue({ limit: 1, now: Date.now(), workerId: 'w' });
		await store.fail(id, { dead: true, error: 'no more retries' });
		const job = await store.get!(id);
		expect(job!.status).toBe('dead');
		expect(mock.zsets.get('absolutejs:queue:claimed')!.size).toBe(0);
		expect(mock.zsets.get('absolutejs:queue:due')!.size).toBe(0);
	});
});

describe('reapStuck', () => {
	test('moves expired-lease claimed jobs back to due', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { a: 1, b: 2 },
			runAt: 500 // due before our claim time
		});
		await store.claimDue({ limit: 1, now: 1000, workerId: 'w' });
		// Mock claimDue stores score = now (1000). reapStuck at now=2000
		// finds score 1000 <= 2000 and reaps.
		const reaped = await store.reapStuck({ leaseMs: 500, now: 2000 });
		expect(reaped).toBe(1);
		const job = await store.get!(id);
		expect(job!.status).toBe('pending');
		expect(job!.lockedAt).toBeUndefined();
		expect(mock.zsets.get('absolutejs:queue:due')!.has(id)).toBe(true);
	});

	test('returns 0 when no lease has expired', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		await store.enqueue({ kind: 'math.add', payload: { a: 1, b: 2 } });
		await store.claimDue({ limit: 1, now: 2000, workerId: 'w' });
		// reapStuck at now=1500 with leaseMs=any: score 2000 > 1500, no
		// reap.
		const reaped = await store.reapStuck({ leaseMs: 5000, now: 1500 });
		expect(reaped).toBe(0);
	});
});

describe('idempotency', () => {
	test('same idempotencyKey returns the existing id', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		const first = await store.enqueue({
			idempotencyKey: 'tx-12345',
			kind: 'math.add',
			payload: { a: 1, b: 2 }
		});
		const second = await store.enqueue({
			idempotencyKey: 'tx-12345',
			kind: 'math.add',
			payload: { a: 99, b: 99 }
		});
		expect(second).toBe(first);
		expect(mock.zsets.get('absolutejs:queue:due')!.size).toBe(1);
	});
});

describe('countByStatus', () => {
	test('counts pending + claimed; v0.0.1 zeros for done/dead/canceled', async () => {
		const mock = makeMockRedis();
		const store = createRedisJobStore({ client: mock.client });
		await store.enqueue({ kind: 'math.add', payload: { a: 1, b: 2 } });
		await store.enqueue({ kind: 'math.add', payload: { a: 3, b: 4 } });
		await store.claimDue({ limit: 1, now: Date.now(), workerId: 'w' });
		const counts = await store.countByStatus!();
		expect(counts).toEqual({
			canceled: 0,
			claimed: 1,
			dead: 0,
			done: 0,
			pending: 1
		});
	});
});

describe('custom keyPrefix', () => {
	test('isolates two stores on the same Redis', async () => {
		const mock = makeMockRedis();
		const a = createRedisJobStore({
			client: mock.client,
			keyPrefix: 'tenant-A:queue:'
		});
		const b = createRedisJobStore({
			client: mock.client,
			keyPrefix: 'tenant-B:queue:'
		});
		await a.enqueue({ kind: 'math.add', payload: { a: 1, b: 2 } });
		await b.enqueue({ kind: 'math.add', payload: { a: 3, b: 4 } });
		expect(mock.zsets.get('tenant-A:queue:due')!.size).toBe(1);
		expect(mock.zsets.get('tenant-B:queue:due')!.size).toBe(1);
	});
});
