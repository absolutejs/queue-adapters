import { defineJobs, t } from '@absolutejs/queue';
import { SQL } from 'bun';
import { describe, expect, test } from 'bun:test';
import { eq, sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/bun-sql';
import { queueJobsTable } from '../src/schema';
import { buildPostgresJobStore } from '../src/store';

const databaseUrl = process.env.QUEUE_TEST_BUN_URL ?? process.env.DATABASE_URL;
const integrationTest = databaseUrl ? test : test.skip;
const ROLLBACK = Symbol('rollback');
const jobs = defineJobs({
	'math.add': t.Object({ left: t.Number(), right: t.Number() })
});

describe('Bun SQL Drizzle compatibility', () => {
	integrationTest('stores payloads as native JSONB objects', async () => {
		const client = new SQL(databaseUrl!);
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
			await client.close();
		}
	});
});
