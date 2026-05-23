import { createJobId, DEFAULT_MAX_ATTEMPTS } from '@absolutejs/queue';
import type {
	Job,
	JobId,
	JobMap,
	JobStatus,
	JobStore
} from '@absolutejs/queue';
import { and, asc, desc, eq, inArray, isNotNull, lte, sql } from 'drizzle-orm';
import type { PgDatabase } from 'drizzle-orm/pg-core';
import { queueJobsTable, type QueueJobRow } from './schema';

// Accepts any Drizzle Postgres database (postgres-js, node-postgres, pglite…).
type AnyPgDatabase = PgDatabase<any, any, any>;

const toJob = <Jobs extends JobMap>(row: QueueJobRow): Job<Jobs> => ({
	attempts: row.attempts,
	createdAt: row.createdAt,
	id: row.id as JobId,
	idempotencyKey: row.idempotencyKey ?? undefined,
	kind: row.kind as keyof Jobs,
	lastError: row.lastError ?? undefined,
	lockedAt: row.lockedAt ?? undefined,
	lockedBy: row.lockedBy ?? undefined,
	maxAttempts: row.maxAttempts,
	payload: row.payload as Jobs[keyof Jobs],
	runAt: row.runAt,
	status: row.status,
	updatedAt: row.updatedAt
});

export const buildPostgresJobStore = <Jobs extends JobMap>(
	db: AnyPgDatabase
): JobStore<Jobs> => ({
	claimDue: async ({ limit, now, workerId }) =>
		db.transaction(async (tx) => {
			const due = await tx
				.select()
				.from(queueJobsTable)
				.where(
					and(
						eq(queueJobsTable.status, 'pending'),
						lte(queueJobsTable.runAt, now)
					)
				)
				.orderBy(asc(queueJobsTable.runAt))
				.limit(limit)
				.for('update', { skipLocked: true });

			if (due.length === 0) return [];

			const ids = due.map((row) => row.id);
			await tx
				.update(queueJobsTable)
				.set({
					lockedAt: now,
					lockedBy: workerId,
					status: 'claimed',
					updatedAt: now
				})
				.where(inArray(queueJobsTable.id, ids));

			return due.map((row) =>
				toJob<Jobs>({
					...row,
					lockedAt: now,
					lockedBy: workerId,
					status: 'claimed',
					updatedAt: now
				})
			);
		}),
	complete: async (id) => {
		await db
			.update(queueJobsTable)
			.set({ status: 'done', updatedAt: Date.now() })
			.where(eq(queueJobsTable.id, id));
	},
	enqueue: async (input) => {
		const now = Date.now();
		const id = createJobId();
		const values = {
			attempts: 0,
			createdAt: now,
			id,
			idempotencyKey: input.idempotencyKey ?? null,
			kind: input.kind as string,
			maxAttempts: input.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
			payload: input.payload,
			runAt: input.runAt ?? now,
			status: 'pending' as const,
			updatedAt: now
		};

		if (input.idempotencyKey === undefined) {
			await db.insert(queueJobsTable).values(values);

			return id;
		}

		// Atomic dedupe: the partial unique index guarantees one active job per
		// key even under concurrent enqueues. On conflict, return the existing one.
		const inserted = await db
			.insert(queueJobsTable)
			.values(values)
			.onConflictDoNothing({
				target: queueJobsTable.idempotencyKey,
				where: sql`${queueJobsTable.status} in ('pending', 'claimed')`
			})
			.returning({ id: queueJobsTable.id });
		if (inserted[0]) return inserted[0].id as JobId;

		const [existing] = await db
			.select({ id: queueJobsTable.id })
			.from(queueJobsTable)
			.where(
				and(
					eq(queueJobsTable.idempotencyKey, input.idempotencyKey),
					inArray(queueJobsTable.status, ['pending', 'claimed'])
				)
			)
			.limit(1);

		return (existing?.id ?? id) as JobId;
	},
	fail: async (id, { dead, error, retryAt }) => {
		await db
			.update(queueJobsTable)
			.set({
				attempts: sql`${queueJobsTable.attempts} + 1`,
				lastError: error,
				lockedAt: null,
				lockedBy: null,
				status: dead ? 'dead' : 'pending',
				updatedAt: Date.now(),
				...(retryAt === undefined ? {} : { runAt: retryAt })
			})
			.where(eq(queueJobsTable.id, id));
	},
	listByKind: async (kind, options) => {
		const rows = await db
			.select()
			.from(queueJobsTable)
			.where(
				options?.status === undefined
					? eq(queueJobsTable.kind, kind as string)
					: and(
							eq(queueJobsTable.kind, kind as string),
							eq(queueJobsTable.status, options.status)
						)
			)
			.limit(options?.limit ?? 1000);

		return rows.map((row) => toJob<Jobs>(row)) as Job<Jobs, typeof kind>[];
	},
	cancel: async (id) => {
		const updated = await db
			.update(queueJobsTable)
			.set({
				lockedAt: null,
				lockedBy: null,
				status: 'canceled',
				updatedAt: Date.now()
			})
			.where(
				and(
					eq(queueJobsTable.id, id),
					inArray(queueJobsTable.status, ['pending', 'claimed'])
				)
			)
			.returning({ id: queueJobsTable.id });

		return updated.length > 0;
	},
	countByStatus: async () => {
		const rows = await db
			.select({
				count: sql<number>`count(*)::int`,
				status: queueJobsTable.status
			})
			.from(queueJobsTable)
			.groupBy(queueJobsTable.status);
		const counts: Record<JobStatus, number> = {
			canceled: 0,
			claimed: 0,
			dead: 0,
			done: 0,
			pending: 0
		};
		for (const row of rows) counts[row.status] = Number(row.count);

		return counts;
	},
	get: async (id) => {
		const [row] = await db
			.select()
			.from(queueJobsTable)
			.where(eq(queueJobsTable.id, id))
			.limit(1);

		return row ? toJob<Jobs>(row) : undefined;
	},
	list: async (options) => {
		const condition = and(
			options?.status === undefined
				? undefined
				: eq(queueJobsTable.status, options.status),
			options?.kind === undefined
				? undefined
				: eq(queueJobsTable.kind, options.kind)
		);
		const rows = await db
			.select()
			.from(queueJobsTable)
			.where(condition)
			.orderBy(desc(queueJobsTable.createdAt))
			.limit(options?.limit ?? 100)
			.offset(options?.offset ?? 0);

		return rows.map((row) => toJob<Jobs>(row));
	},
	retry: async (id) => {
		const updated = await db
			.update(queueJobsTable)
			.set({
				attempts: 0,
				lastError: null,
				lockedAt: null,
				lockedBy: null,
				runAt: Date.now(),
				status: 'pending',
				updatedAt: Date.now()
			})
			.where(eq(queueJobsTable.id, id))
			.returning({ id: queueJobsTable.id });

		return updated.length > 0;
	},
	reapStuck: async ({ leaseMs, now }) => {
		const reaped = await db
			.update(queueJobsTable)
			.set({
				lockedAt: null,
				lockedBy: null,
				status: 'pending',
				updatedAt: now
			})
			.where(
				and(
					eq(queueJobsTable.status, 'claimed'),
					isNotNull(queueJobsTable.lockedAt),
					lte(sql`${queueJobsTable.lockedAt} + ${leaseMs}`, now)
				)
			)
			.returning({ id: queueJobsTable.id });

		return reaped.length;
	}
});
