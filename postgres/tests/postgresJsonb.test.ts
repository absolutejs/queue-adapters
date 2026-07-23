import { defineJobs, t } from '@absolutejs/queue';
import { describe, expect, test } from 'bun:test';
import { eq, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { queueJobsTable } from '../src/schema';
import { buildPostgresJobStore } from '../src/store';

const databaseUrl =
	process.env.QUEUE_TEST_POSTGRES_URL ?? process.env.DATABASE_URL;
const integrationTest = databaseUrl ? test : test.skip;
const ROLLBACK = Symbol('rollback');
const jobs = defineJobs({
	'math.add': t.Object({ left: t.Number(), right: t.Number() })
});

describe('postgres.js Drizzle compatibility', () => {
	integrationTest('stores payloads as native JSONB objects', async () => {
		const client = postgres(databaseUrl!, { max: 1, prepare: false });
		const db = drizzle({ client });
		try {
			await db.transaction(async (transaction) => {
				const store = buildPostgresJobStore(transaction, jobs);
				const id = await store.enqueue({
					kind: 'math.add',
					payload: { left: 2, right: 3 }
				});
				const [stored] = await transaction
					.select({
						payload: queueJobsTable.payload,
						type: sql<string>`jsonb_typeof(${queueJobsTable.payload})`
					})
					.from(queueJobsTable)
					.where(eq(queueJobsTable.id, id))
					.limit(1);

				expect(stored).toEqual({
					payload: { left: 2, right: 3 },
					type: 'object'
				});
				throw ROLLBACK;
			});
		} catch (error) {
			if (error !== ROLLBACK) throw error;
		} finally {
			await client.end();
		}
	});
});
