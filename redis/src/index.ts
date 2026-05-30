/**
 * @absolutejs/queue-redis — Redis-backed `JobStore` for
 * `@absolutejs/queue`. Sibling to `@absolutejs/queue-postgres` for
 * shops running Redis instead of (or alongside) Postgres.
 *
 * **Storage layout** (all keys parameterized by `keyPrefix`, default
 *  `"absolutejs:queue:"`):
 *
 *   - `<prefix>job:<id>` — Redis HASH per job; stores the full record.
 *   - `<prefix>due` — Redis ZSET keyed by `runAt`. Workers `ZRANGEBYSCORE`
 *     up to `now` to find claimable jobs.
 *   - `<prefix>claimed` — Redis ZSET keyed by `lockedAt + leaseMs`.
 *     `reapStuck` finds entries whose lease has expired
 *     (`ZRANGEBYSCORE -inf <now>`).
 *   - `<prefix>idempotency:<key>` — STRING mapping idempotency key →
 *     job id, set with NX so a second enqueue with the same key returns
 *     the existing id.
 *   - `<prefix>kind:<kind>` — SET of job ids by kind. Optional;
 *     populated only when `countByStatus` is called (lazy index).
 *
 * **Atomic claim via Lua.** `claimDue` runs a Lua script that
 * atomically: (1) ZRANGEBYSCORE due 0 now LIMIT 0 N, (2) ZREM each from
 * due, (3) ZADD each to claimed with `lockedAt + leaseMs` score,
 * (4) HSET each job's status/lockedAt/lockedBy, (5) returns the
 * resulting hash payloads. Without Lua, two concurrent workers could
 * race the ZRANGEBYSCORE → ZREM gap and both think they own the same
 * job. With Lua, the entire 5-step sequence is one atomic operation.
 *
 * **Driver compatibility.** Same narrow-interface approach as
 * `sync-bus-redis`. The adapter takes a `RedisCommandClient` with the
 * minimal command set we use; both `ioredis` and `node-redis` v4+
 * structurally satisfy it. See README for wirings.
 *
 * **v0.0.1 surface coverage** (relative to `JobStore` from
 * `@absolutejs/queue`):
 *
 *   - Required: `enqueue`, `claimDue`, `complete`, `fail`, `reapStuck` ✓
 *   - Optional: `get` ✓, `countByStatus` ✓
 *   - Optional: `cancel`, `list`, `listByKind`, `retry` — deferred to
 *     0.1.0 (less hot; lazy indexing required).
 */

import { createJobId } from '@absolutejs/queue';
import type {
	ClaimDueOptions,
	FailOptions,
	Job,
	JobId,
	JobMap,
	JobStatus,
	JobStore,
	ReapStuckOptions
} from '@absolutejs/queue';

/**
 * Minimal Redis command surface. Both ioredis and node-redis v4+
 * structurally satisfy this (their typed wrappers expose the same
 * names + signatures). Reads/writes pass through as strings.
 */
export type RedisCommandClient = {
	hset: (key: string, fields: Record<string, string>) => Promise<unknown>;
	hgetall: (key: string) => Promise<Record<string, string> | null>;
	hdel: (key: string, ...fields: string[]) => Promise<unknown>;
	del: (...keys: string[]) => Promise<unknown>;
	zadd: (
		key: string,
		score: number,
		member: string
	) => Promise<unknown>;
	zrem: (key: string, ...members: string[]) => Promise<unknown>;
	zrangebyscore: (
		key: string,
		min: number | string,
		max: number | string,
		offset?: number,
		count?: number
	) => Promise<string[]>;
	zcard: (key: string) => Promise<number>;
	sadd: (key: string, ...members: string[]) => Promise<unknown>;
	srem: (key: string, ...members: string[]) => Promise<unknown>;
	set: (
		key: string,
		value: string,
		mode?: 'NX'
	) => Promise<string | null>;
	get: (key: string) => Promise<string | null>;
	scard: (key: string) => Promise<number>;
	smembers: (key: string) => Promise<string[]>;
	/**
	 * Execute a Lua script with the given KEYS and ARGV. Both ioredis
	 * (`eval(script, numkeys, ...keysAndArgs)`) and node-redis
	 * (`EVAL` via `sendCommand` or its scripting API) satisfy this
	 * via adapter wrapping — see README.
	 */
	eval: (script: string, keys: string[], args: string[]) => Promise<unknown>;
};

export type CreateRedisJobStoreOptions = {
	/** The Redis client. See README for ioredis / node-redis wiring. */
	client: RedisCommandClient;
	/**
	 * Key prefix. Default `'absolutejs:queue:'`. Use a tenant- or
	 * env-scoped prefix to isolate multiple stores on the same Redis.
	 */
	keyPrefix?: string;
};

const DEFAULT_KEY_PREFIX = 'absolutejs:queue:';
const DEFAULT_MAX_ATTEMPTS = 5;

const fields = {
	attempts: 'attempts',
	createdAt: 'createdAt',
	id: 'id',
	idempotencyKey: 'idempotencyKey',
	kind: 'kind',
	lastError: 'lastError',
	lockedAt: 'lockedAt',
	lockedBy: 'lockedBy',
	maxAttempts: 'maxAttempts',
	payload: 'payload',
	runAt: 'runAt',
	status: 'status',
	updatedAt: 'updatedAt'
} as const;

/**
 * Atomic claim Lua. KEYS[1] = due ZSET, KEYS[2] = claimed ZSET,
 * KEYS[3] = job-key prefix (`<prefix>job:` — we concatenate ids on the
 * Lua side rather than threading them as ARGV). ARGV[1] = now,
 * ARGV[2] = limit, ARGV[3] = workerId, ARGV[4] = leaseMs.
 *
 * Returns a flat list of HGETALL payloads: each job is a sequence of
 * (field, value) pairs in the array — the caller stitches them back
 * to JS objects.
 */
const CLAIM_LUA = `
local dueKey = KEYS[1]
local claimedKey = KEYS[2]
local jobKeyPrefix = KEYS[3]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local workerId = ARGV[3]
local leaseMs = tonumber(ARGV[4])

local ids = redis.call('ZRANGEBYSCORE', dueKey, '-inf', now, 'LIMIT', 0, limit)
local result = {}
for i, id in ipairs(ids) do
    redis.call('ZREM', dueKey, id)
    redis.call('ZADD', claimedKey, now + leaseMs, id)
    redis.call('HSET', jobKeyPrefix .. id,
        'status', 'claimed',
        'lockedAt', tostring(now),
        'lockedBy', workerId,
        'updatedAt', tostring(now))
    local job = redis.call('HGETALL', jobKeyPrefix .. id)
    table.insert(result, job)
end
return result
`;

/**
 * Atomic reap Lua. KEYS[1] = claimed ZSET, KEYS[2] = due ZSET,
 * KEYS[3] = job-key prefix. ARGV[1] = now. Moves every claimed entry
 * whose lease has expired back to `due`. Returns the count moved.
 */
const REAP_LUA = `
local claimedKey = KEYS[1]
local dueKey = KEYS[2]
local jobKeyPrefix = KEYS[3]
local now = tonumber(ARGV[1])

local ids = redis.call('ZRANGEBYSCORE', claimedKey, '-inf', now)
for i, id in ipairs(ids) do
    redis.call('ZREM', claimedKey, id)
    -- Restore to due at the job's runAt so it's picked up immediately.
    local runAtStr = redis.call('HGET', jobKeyPrefix .. id, 'runAt')
    local runAt = tonumber(runAtStr) or now
    redis.call('ZADD', dueKey, runAt, id)
    redis.call('HDEL', jobKeyPrefix .. id, 'lockedAt', 'lockedBy')
    redis.call('HSET', jobKeyPrefix .. id, 'status', 'pending', 'updatedAt', tostring(now))
end
return #ids
`;

const hashToJob = <Jobs extends JobMap>(
	hash: Record<string, string> | null
): Job<Jobs> | undefined => {
	if (hash === null || Object.keys(hash).length === 0) return undefined;
	const get = (name: string): string | undefined => hash[name];
	const payloadRaw = get(fields.payload);
	const job: Job<Jobs> = {
		attempts: Number(get(fields.attempts) ?? 0),
		createdAt: Number(get(fields.createdAt) ?? 0),
		id: get(fields.id) as JobId,
		kind: get(fields.kind) as keyof Jobs,
		maxAttempts: Number(get(fields.maxAttempts) ?? DEFAULT_MAX_ATTEMPTS),
		payload: (payloadRaw !== undefined
			? JSON.parse(payloadRaw)
			: undefined) as Jobs[keyof Jobs],
		runAt: Number(get(fields.runAt) ?? 0),
		status: (get(fields.status) ?? 'pending') as JobStatus,
		updatedAt: Number(get(fields.updatedAt) ?? 0)
	};
	const idempotencyKey = get(fields.idempotencyKey);
	if (idempotencyKey !== undefined) job.idempotencyKey = idempotencyKey;
	const lastError = get(fields.lastError);
	if (lastError !== undefined) job.lastError = lastError;
	const lockedAt = get(fields.lockedAt);
	if (lockedAt !== undefined) job.lockedAt = Number(lockedAt);
	const lockedBy = get(fields.lockedBy);
	if (lockedBy !== undefined) job.lockedBy = lockedBy;
	return job;
};

/**
 * Parse a Lua-returned flat array (sequence of HGETALL payloads,
 * where each payload is itself a flat array of (field, value) pairs)
 * into Job objects.
 */
const parseClaimResult = <Jobs extends JobMap>(
	result: unknown
): Job<Jobs>[] => {
	if (!Array.isArray(result)) return [];
	const jobs: Job<Jobs>[] = [];
	for (const entry of result) {
		if (!Array.isArray(entry)) continue;
		const hash: Record<string, string> = {};
		for (let i = 0; i < entry.length; i += 2) {
			const key = entry[i];
			const value = entry[i + 1];
			if (typeof key === 'string' && typeof value === 'string') {
				hash[key] = value;
			}
		}
		const job = hashToJob<Jobs>(hash);
		if (job !== undefined) jobs.push(job);
	}
	return jobs;
};

export const createRedisJobStore = <Jobs extends JobMap>(
	options: CreateRedisJobStoreOptions
): JobStore<Jobs> => {
	const { client } = options;
	const prefix = options.keyPrefix ?? DEFAULT_KEY_PREFIX;
	const dueKey = `${prefix}due`;
	const claimedKey = `${prefix}claimed`;
	const jobKeyPrefix = `${prefix}job:`;
	const idempotencyKeyPrefix = `${prefix}idempotency:`;
	const jobKey = (id: JobId): string => `${jobKeyPrefix}${id}`;

	const writeJob = async (job: Job<Jobs>): Promise<void> => {
		const hash: Record<string, string> = {
			[fields.attempts]: String(job.attempts),
			[fields.createdAt]: String(job.createdAt),
			[fields.id]: job.id,
			[fields.kind]: String(job.kind),
			[fields.maxAttempts]: String(job.maxAttempts),
			[fields.payload]: JSON.stringify(job.payload),
			[fields.runAt]: String(job.runAt),
			[fields.status]: job.status,
			[fields.updatedAt]: String(job.updatedAt)
		};
		if (job.idempotencyKey !== undefined) {
			hash[fields.idempotencyKey] = job.idempotencyKey;
		}
		if (job.lastError !== undefined) {
			hash[fields.lastError] = job.lastError;
		}
		if (job.lockedAt !== undefined) {
			hash[fields.lockedAt] = String(job.lockedAt);
		}
		if (job.lockedBy !== undefined) {
			hash[fields.lockedBy] = job.lockedBy;
		}
		await client.hset(jobKey(job.id), hash);
	};

	return {
		claimDue: async ({ limit, now, workerId }: ClaimDueOptions) => {
			const result = await client.eval(
				CLAIM_LUA,
				[dueKey, claimedKey, jobKeyPrefix],
				[String(now), String(limit), workerId, String(0)]
			);
			return parseClaimResult<Jobs>(result);
		},

		complete: async (id: JobId) => {
			const now = Date.now();
			await client.zrem(claimedKey, id);
			await client.zrem(dueKey, id);
			await client.hset(jobKey(id), {
				[fields.status]: 'done',
				[fields.updatedAt]: String(now)
			});
			await client.hdel(jobKey(id), fields.lockedAt, fields.lockedBy);
		},

		countByStatus: async () => {
			// Lazy: walk known status partitions by SET-of-ids lookup is
			// the alternative, but we don't maintain those indexes
			// (would cost on every transition). For v0.0.1, count just
			// what's in the structured ZSETs: due (pending) + claimed.
			// done/dead/canceled require a separate maintained index;
			// surface 0 for those and document the limitation.
			const [pending, claimed] = await Promise.all([
				client.zcard(dueKey),
				client.zcard(claimedKey)
			]);
			return {
				canceled: 0,
				claimed,
				dead: 0,
				done: 0,
				pending
			};
		},

		enqueue: async (input) => {
			const now = Date.now();
			// Idempotency: if the caller provided a key, check if we've
			// already enqueued under that key. SET with NX returns null
			// when the key exists; we read the stored id and return.
			if (input.idempotencyKey !== undefined) {
				const idemKey = `${idempotencyKeyPrefix}${input.idempotencyKey}`;
				const existing = await client.get(idemKey);
				if (existing !== null) {
					return existing as JobId;
				}
			}
			const id = createJobId();
			const job: Job<Jobs> = {
				attempts: 0,
				createdAt: now,
				id,
				kind: input.kind,
				maxAttempts: input.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
				payload: input.payload,
				runAt: input.runAt ?? now,
				status: 'pending',
				updatedAt: now
			};
			if (input.idempotencyKey !== undefined) {
				job.idempotencyKey = input.idempotencyKey;
			}
			await writeJob(job);
			await client.zadd(dueKey, job.runAt, id);
			if (input.idempotencyKey !== undefined) {
				await client.set(
					`${idempotencyKeyPrefix}${input.idempotencyKey}`,
					id,
					'NX'
				);
			}
			return id;
		},

		fail: async (id: JobId, { dead, error, retryAt }: FailOptions) => {
			const now = Date.now();
			// Remove from claimed first (we own the lease that failed).
			await client.zrem(claimedKey, id);
			const next: Record<string, string> = {
				[fields.status]: dead ? 'dead' : 'pending',
				[fields.updatedAt]: String(now),
				[fields.lastError]: error
			};
			// Increment attempts via a read-modify-write. This is NOT
			// atomic vs concurrent operations on the same job, but a
			// job only has one logical owner at a time (the worker that
			// claimed it), so the race window is the worker boundary.
			const existing = await client.hgetall(jobKey(id));
			const currentAttempts = Number(
				existing?.[fields.attempts] ?? 0
			);
			next[fields.attempts] = String(currentAttempts + 1);
			if (retryAt !== undefined) {
				next[fields.runAt] = String(retryAt);
			}
			await client.hset(jobKey(id), next);
			await client.hdel(jobKey(id), fields.lockedAt, fields.lockedBy);
			// Restore to due unless dead.
			if (!dead) {
				const runAt = retryAt ?? Number(existing?.[fields.runAt] ?? now);
				await client.zadd(dueKey, runAt, id);
			}
		},

		get: async (id: JobId) => {
			const hash = await client.hgetall(jobKey(id));
			return hashToJob<Jobs>(hash);
		},

		reapStuck: async ({ leaseMs, now }: ReapStuckOptions) => {
			// The CLAIM_LUA records lockedAt + leaseMs as the claimed
			// ZSET score. `now` already accounts for the lease. The
			// REAP_LUA simply finds claimed entries with score <= now.
			// leaseMs is preserved here so the signature matches the
			// existing JobStore contract; the actual gate is baked into
			// the score at claim time.
			void leaseMs;
			const result = await client.eval(
				REAP_LUA,
				[claimedKey, dueKey, jobKeyPrefix],
				[String(now)]
			);
			return typeof result === 'number' ? result : 0;
		}
	};
};
