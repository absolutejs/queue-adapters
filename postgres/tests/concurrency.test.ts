import { describe, expect, it } from 'bun:test';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { queueSchema } from '../src/schema';
import { buildPostgresJobStore } from '../src/store';

// True cross-worker contention can't be tested on PGlite (single connection).
// This runs against a real Postgres when QUEUE_TEST_DATABASE_URL is set
// (e.g. a docker container in CI); otherwise it's skipped.
type Jobs = { 'load.test': { index: number } };

const url = process.env.QUEUE_TEST_DATABASE_URL;
const integration = url ? it : it.skip;

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

describe('@absolutejs/queue-postgres (concurrency)', () => {
	integration(
		'claims each job exactly once across parallel workers',
		async () => {
			const client = postgres(url as string, { prepare: false });
			const db = drizzle(client, { schema: queueSchema });
			await db.execute(DDL);
			await db.execute(sql`TRUNCATE queue_jobs`);
			const store = buildPostgresJobStore<Jobs>(db);

			const total = 200;
			const claimAt = Date.now() + 1000;
			for (let index = 0; index < total; index += 1)
				await store.enqueue({
					kind: 'load.test',
					payload: { index }
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

			await client.end();
		}
	);
});
