import { PGlite } from '@electric-sql/pglite';
import { describe, expect, it } from 'bun:test';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/pglite';
import { queueSchema } from '../src/schema';
import { buildPostgresJobStore } from '../src/store';

type Jobs = { 'math.add': { left: number; right: number } };

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
		locked_at bigint,
		locked_by text,
		last_error text,
		created_at bigint NOT NULL,
		updated_at bigint NOT NULL
	)
`;

const setup = async () => {
	const db = drizzle(new PGlite(), { schema: queueSchema });
	await db.execute(DDL);

	return buildPostgresJobStore<Jobs>(db);
};

describe('@absolutejs/queue-postgres admin methods', () => {
	it('lists, counts, gets, cancels and retries', async () => {
		const store = await setup();
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

		expect(await store.list?.({ status: 'pending' })).toHaveLength(1);
		expect(await store.list?.({ status: 'dead' })).toHaveLength(0);
	});
});
