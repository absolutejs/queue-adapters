import { PGlite } from '@electric-sql/pglite';
import { describe, expect, it } from 'bun:test';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/pglite';
import { queueSchema } from '../src/schema';
import { buildPostgresJobStore } from '../src/store';

type Jobs = {
	'always.fail': { reason: string };
	'math.add': { left: number; right: number };
};

const CREATE_TABLE = sql`
	CREATE TABLE IF NOT EXISTS queue_jobs (
		id varchar(255) PRIMARY KEY,
		kind text NOT NULL,
		payload jsonb NOT NULL,
		status text NOT NULL DEFAULT 'pending',
		run_at bigint NOT NULL,
		attempts integer NOT NULL DEFAULT 0,
		max_attempts integer NOT NULL,
		idempotency_key text,
		locked_at bigint,
		locked_by text,
		last_error text,
		created_at bigint NOT NULL,
		updated_at bigint NOT NULL
	)
`;

const CREATE_IDEMPOTENCY_INDEX = sql`
	CREATE UNIQUE INDEX IF NOT EXISTS queue_jobs_idempotency_active_idx
	ON queue_jobs (idempotency_key)
	WHERE status IN ('pending', 'claimed')
`;

const setup = async () => {
	const db = drizzle(new PGlite(), { schema: queueSchema });
	await db.execute(CREATE_TABLE);
	await db.execute(CREATE_IDEMPOTENCY_INDEX);

	return buildPostgresJobStore<Jobs>(db);
};

describe('@absolutejs/queue-postgres', () => {
	it('enqueues, claims a due job, and completes it', async () => {
		const store = await setup();
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
		const store = await setup();
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

	it('increments attempts on retry and dead-letters', async () => {
		const store = await setup();
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
		const store = await setup();
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
});
