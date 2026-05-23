import type { JobStatus } from '@absolutejs/queue';
import { sql } from 'drizzle-orm';
import {
	bigint,
	index,
	integer,
	jsonb,
	pgTable,
	text,
	uniqueIndex,
	varchar
} from 'drizzle-orm/pg-core';

export const queueJobsTable = pgTable(
	'queue_jobs',
	{
		id: varchar('id', { length: 255 }).primaryKey(),
		kind: text('kind').notNull(),
		payload: jsonb('payload').$type<unknown>().notNull(),
		status: text('status').$type<JobStatus>().notNull().default('pending'),
		runAt: bigint('run_at', { mode: 'number' }).notNull(),
		attempts: integer('attempts').notNull().default(0),
		maxAttempts: integer('max_attempts').notNull(),
		idempotencyKey: text('idempotency_key'),
		lockedAt: bigint('locked_at', { mode: 'number' }),
		lockedBy: text('locked_by'),
		lastError: text('last_error'),
		createdAt: bigint('created_at', { mode: 'number' }).notNull(),
		updatedAt: bigint('updated_at', { mode: 'number' }).notNull()
	},
	(table) => [
		index('queue_jobs_due_idx').on(table.status, table.runAt),
		// One active job per idempotency key — the hard race guard.
		uniqueIndex('queue_jobs_idempotency_active_idx')
			.on(table.idempotencyKey)
			.where(sql`${table.status} in ('pending', 'claimed')`)
	]
);

export const queueSchema = { queueJobs: queueJobsTable };

export type QueueJobRow = typeof queueJobsTable.$inferSelect;
export type QueueJobInsert = typeof queueJobsTable.$inferInsert;
