import {
	defineJobs,
	t,
	type JobMapFromDefinition,
	type JobStore
} from '@absolutejs/queue';
import {
	afterAll,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it
} from 'bun:test';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { buildPostgresJobStore } from '../src/store';

// Integration tests run against a real Postgres (the production driver, postgres.js).
// Set QUEUE_TEST_DATABASE_URL to run them; otherwise the suite is skipped.
const jobs = defineJobs({
	'always.fail': t.Object({ reason: t.String() }),
	'math.add': t.Object({ left: t.Number(), right: t.Number() })
});
type Jobs = JobMapFromDefinition<typeof jobs>;

const url = process.env.QUEUE_TEST_DATABASE_URL;
const suite = url ? describe : describe.skip;

const DDL = sql`
	CREATE TABLE IF NOT EXISTS queue_jobs (
		id varchar(255) PRIMARY KEY,
		kind text NOT NULL,
		payload jsonb NOT NULL,
		status text NOT NULL DEFAULT 'pending',
		run_at bigint NOT NULL,
		attempts integer NOT NULL DEFAULT 0,
		max_attempts integer NOT NULL,
		idempotency_key text,
		claim_token text,
		locked_at bigint,
		locked_by text,
		last_error text,
		created_at bigint NOT NULL,
		updated_at bigint NOT NULL
	)
`;

const INDEX = sql`
	CREATE UNIQUE INDEX IF NOT EXISTS queue_jobs_idempotency_active_idx
	ON queue_jobs (idempotency_key)
	WHERE status IN ('pending', 'claimed')
`;

suite('@absolutejs/queue-postgres', () => {
	let client: ReturnType<typeof postgres>;
	let store: JobStore<Jobs>;

	beforeAll(async () => {
		client = postgres(url as string, { prepare: false });
		const db = drizzle({ client });
		await db.execute(DDL);
		await db.execute(INDEX);
		store = buildPostgresJobStore(db, jobs);
	});

	beforeEach(async () => {
		await client`TRUNCATE queue_jobs`;
	});

	afterAll(async () => {
		await client.end();
	});

	it('filters supported kinds before the claim limit and leaves foreign jobs untouched', async () => {
		const newer = await store.enqueue({
			kind: 'always.fail',
			payload: { reason: 'owned by newer worker' },
			runAt: 1
		});
		const older = await store.enqueue({
			kind: 'math.add',
			payload: { left: 2, right: 3 },
			runAt: 2
		});
		const claimed = await store.claimDue({
			kinds: ['math.add'],
			limit: 1,
			now: Date.now(),
			workerId: 'older-worker'
		});
		expect(claimed.map((job) => job.id)).toEqual([older]);
		expect((await store.get?.(newer))?.status).toBe('pending');
		expect((await store.get?.(newer))?.attempts).toBe(0);
		expect(
			await store.claimDue({
				kinds: [],
				limit: 1,
				now: Date.now(),
				workerId: 'no-handlers'
			})
		).toEqual([]);
		const narrow = buildPostgresJobStore(
			drizzle({ client }),
			defineJobs({ 'math.add': jobs['math.add'] })
		);
		expect(
			await narrow.claimDue({
				limit: 1,
				now: Date.now(),
				workerId: 'narrow-definition'
			})
		).toEqual([]);
		expect((await store.get?.(newer))?.status).toBe('pending');
		const upgraded = await store.claimDue({
			kinds: ['always.fail'],
			limit: 1,
			now: Date.now(),
			workerId: 'upgraded-worker'
		});
		expect(upgraded.map((job) => job.id)).toEqual([newer]);
	});

	it('enqueues, claims a due job, and completes it', async () => {
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { left: 2, right: 3 }
		});

		const claimed = await store.claimDue({
			limit: 10,
			now: Date.now() + 1000,
			workerId: 'worker-1'
		});

		expect(claimed.length).toBe(1);
		expect(claimed[0]?.id).toBe(id);
		expect(claimed[0]?.status).toBe('claimed');
		expect(claimed[0]?.payload).toEqual({ left: 2, right: 3 });

		await store.complete(id);
		const done = await store.listByKind?.('math.add', { status: 'done' });
		expect(done?.length).toBe(1);
	});

	it('dedupes enqueue by idempotency key', async () => {
		const first = await store.enqueue({
			idempotencyKey: 'once',
			kind: 'math.add',
			payload: { left: 1, right: 1 }
		});
		const second = await store.enqueue({
			idempotencyKey: 'once',
			kind: 'math.add',
			payload: { left: 9, right: 9 }
		});

		expect(second).toBe(first);
	});

	it('rejects an invalid payload at enqueue', async () => {
		const result = store.enqueue({
			kind: 'math.add',
			// JSON is an intentionally untrusted runtime boundary.
			payload: JSON.parse('{"left":1}')
		});

		await expect(result).rejects.toThrow();
	});

	it('increments attempts on retry and dead-letters', async () => {
		const id = await store.enqueue({
			kind: 'always.fail',
			maxAttempts: 2,
			payload: { reason: 'test' }
		});

		await store.claimDue({
			limit: 1,
			now: Date.now() + 1000,
			workerId: 'w'
		});
		await store.fail(id, { error: 'boom', retryAt: 0 });

		const retried = await store.listByKind?.('always.fail', {
			status: 'pending'
		});
		expect(retried?.[0]?.attempts).toBe(1);

		await store.claimDue({
			limit: 1,
			now: Date.now() + 1000,
			workerId: 'w'
		});
		await store.fail(id, { dead: true, error: 'boom' });

		const dead = await store.listByKind?.('always.fail', {
			status: 'dead'
		});
		expect(dead?.length).toBe(1);
		expect(dead?.[0]?.attempts).toBe(2);
	});

	it('reaps stuck claimed jobs back to pending', async () => {
		await store.enqueue({
			kind: 'math.add',
			payload: { left: 1, right: 2 }
		});
		await store.claimDue({ limit: 1, now: Date.now(), workerId: 'w' });

		const reaped = await store.reapStuck({
			leaseMs: 0,
			now: Date.now() + 1000
		});

		expect(reaped).toBe(1);
		const pending = await store.listByKind?.('math.add', {
			status: 'pending'
		});
		expect(pending?.length).toBe(1);
	});

	it('admin: list, count, get, cancel, retry', async () => {
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { left: 1, right: 2 }
		});

		expect((await store.list?.())?.length).toBe(1);
		expect((await store.get?.(id))?.id).toBe(id);
		expect((await store.countByStatus?.())?.pending).toBe(1);

		expect(await store.cancel?.(id)).toBe(true);
		expect((await store.countByStatus?.())?.canceled).toBe(1);

		expect(await store.retry?.(id)).toBe(true);
		const counts = await store.countByStatus?.();
		expect(counts?.pending).toBe(1);
		expect(counts?.canceled).toBe(0);
	});

	it('claims each job exactly once across parallel workers', async () => {
		const total = 100;
		const claimAt = Date.now() + 1000;
		for (let index = 0; index < total; index += 1)
			await store.enqueue({
				kind: 'math.add',
				payload: { left: index, right: 0 }
			});

		const drain = async (workerId: string) => {
			const ids: string[] = [];
			for (;;) {
				const batch = await store.claimDue({
					limit: 10,
					now: claimAt,
					workerId
				});
				if (batch.length === 0) break;
				for (const job of batch) ids.push(job.id);
			}

			return ids;
		};

		const claimed = (
			await Promise.all([
				drain('w1'),
				drain('w2'),
				drain('w3'),
				drain('w4')
			])
		).flat();

		expect(claimed.length).toBe(total);
		expect(new Set(claimed).size).toBe(total);
	});
	it('rejects stale completions and failures after reclaim with the same worker and clock', async () => {
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { left: 1, right: 2 },
			runAt: 1
		});
		const [first] = await store.claimDue({
			limit: 1,
			now: 10,
			workerId: 'same-worker'
		});
		await store.reapStuck({ now: 10, leaseMs: 0 });
		const [second] = await store.claimDue({
			limit: 1,
			now: 10,
			workerId: 'same-worker'
		});
		expect(first!.claimToken).toBeDefined();
		expect(second!.claimToken).not.toBe(first!.claimToken);
		expect(
			await store.failClaim!(id, first!.claimToken!, {
				dead: true,
				error: 'stale'
			})
		).toBe(false);
		const [stale, current] = await Promise.all([
			store.completeClaim!(id, first!.claimToken!),
			store.completeClaim!(id, second!.claimToken!)
		]);
		expect(stale).toBe(false);
		expect(current).toBe(true);
		expect(
			await store.failClaim!(id, second!.claimToken!, {
				error: 'late failure'
			})
		).toBe(false);
		const done = await store.get!(id);
		expect(done!.status).toBe('done');
		expect(done!.attempts).toBe(1);
	});

	it('only the current claim can schedule a retry, and cancellation rejects its late result', async () => {
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { left: 1, right: 2 },
			runAt: 1
		});
		const [first] = await store.claimDue({
			limit: 1,
			now: 10,
			workerId: 'worker'
		});
		expect(
			await store.failClaim!(id, first!.claimToken!, {
				retryAt: 11,
				error: 'retryable'
			})
		).toBe(true);
		const [second] = await store.claimDue({
			limit: 1,
			now: 11,
			workerId: 'worker'
		});
		expect(
			await store.failClaim!(id, first!.claimToken!, {
				dead: true,
				error: 'late'
			})
		).toBe(false);
		expect((await store.get!(id))!.attempts).toBe(1);
		await store.cancel!(id);
		expect(await store.completeClaim!(id, second!.claimToken!)).toBe(false);
		expect((await store.get!(id))!.status).toBe('canceled');
	});
	it('dead-letters a crashed job when its retry budget is exhausted', async () => {
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { left: 1, right: 2 },
			runAt: 1,
			maxAttempts: 1
		});
		const [claim] = await store.claimDue({
			now: 10,
			limit: 1,
			workerId: 'crashed'
		});
		await store.reapStuck({ now: 11, leaseMs: 1 });
		expect((await store.get!(id))!.status).toBe('dead');
		expect((await store.get!(id))!.attempts).toBe(1);
		expect(
			await store.claimDue({ now: 12, limit: 1, workerId: 'replacement' })
		).toEqual([]);
		expect(await store.completeClaim!(id, claim!.claimToken!)).toBe(false);
	});
});
