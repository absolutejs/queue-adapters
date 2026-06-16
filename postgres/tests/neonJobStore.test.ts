import { defineJobs, t } from '@absolutejs/queue';
import {
	afterAll,
	beforeAll,
	beforeEach,
	describe,
	expect,
	it
} from 'bun:test';
import { Pool, neonConfig } from '@neondatabase/serverless';
import { sql } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/neon-serverless';
import { createNeonJobStore } from '../src/neonJobStore';

// Integration tests against Neon over WebSocket. Set QUEUE_TEST_NEON_URL to a
// Neon branch URL to run them; otherwise the suite is skipped. Use a
// dedicated branch — the test creates + truncates `queue_jobs`.
const jobs = defineJobs({
	'math.add': t.Object({ left: t.Number(), right: t.Number() })
});

const url = process.env.QUEUE_TEST_NEON_URL;
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

suite('createNeonJobStore (Neon serverless WebSocket)', () => {
	let pool: Pool;
	let store: ReturnType<typeof createNeonJobStore<typeof jobs>>;

	beforeAll(async () => {
		// Bun ships a global WebSocket so this is usually a no-op; node + older
		// runtimes may need an explicit ws polyfill.
		if (typeof WebSocket !== 'undefined') {
			neonConfig.webSocketConstructor = WebSocket;
		}
		pool = new Pool({ connectionString: url as string });
		const db = drizzle({ client: pool });
		await db.execute(DDL);
		await db.execute(INDEX);
		store = createNeonJobStore({ jobs, pool });
	});

	beforeEach(async () => {
		await pool.query('TRUNCATE queue_jobs');
	});

	afterAll(async () => {
		await pool.end();
	});

	it('enqueues, claims with FOR UPDATE SKIP LOCKED, completes', async () => {
		const id = await store.enqueue({
			kind: 'math.add',
			payload: { left: 2, right: 3 }
		});

		const claimed = await store.claimDue({
			limit: 10,
			now: Date.now() + 1000,
			workerId: 'neon-worker-1'
		});

		expect(claimed.length).toBe(1);
		expect(claimed[0]?.id).toBe(id);
		expect(claimed[0]?.status).toBe('claimed');

		await store.complete(id);
		const done = await store.listByKind?.('math.add', { status: 'done' });
		expect(done?.length).toBe(1);
	});

	it('dedupes by idempotency key', async () => {
		const first = await store.enqueue({
			idempotencyKey: 'neon-dedupe',
			kind: 'math.add',
			payload: { left: 1, right: 1 }
		});
		const second = await store.enqueue({
			idempotencyKey: 'neon-dedupe',
			kind: 'math.add',
			payload: { left: 9, right: 9 }
		});

		expect(second).toBe(first);
	});

	it('claims each job exactly once across parallel pseudo-workers', async () => {
		const total = 25;
		const claimAt = Date.now() + 1000;
		for (let index = 0; index < total; index += 1) {
			await store.enqueue({
				kind: 'math.add',
				payload: { left: index, right: 0 }
			});
		}

		const drain = async (workerId: string) => {
			const ids: string[] = [];
			for (;;) {
				const batch = await store.claimDue({
					limit: 5,
					now: claimAt,
					workerId
				});
				if (batch.length === 0) break;
				for (const job of batch) ids.push(job.id);
			}

			return ids;
		};

		const claimed = (
			await Promise.all([drain('n1'), drain('n2'), drain('n3')])
		).flat();

		expect(claimed.length).toBe(total);
		expect(new Set(claimed).size).toBe(total);
	});
});
