// `createNeonJobStore` — convenience factory for Neon's WebSocket driver
// (`@neondatabase/serverless`). Mirrors `createPostgresJobStore` (which uses
// postgres.js); use whichever matches your app's primary driver so the queue
// shares its connection pool.
//
// **Important: use the WebSocket driver, not `drizzle-orm/neon-http`.**
// The queue's `claimDue` opens a transaction and selects with
// `FOR UPDATE SKIP LOCKED` — both require row-level locking that
// neon-http's single-statement HTTP API does not support. Neon's
// `@neondatabase/serverless` package exposes both: the HTTP `neon()`
// function for simple queries, and a `Pool` over WebSockets that does
// real transactions. The queue uses the latter.
//
// If your app's main code already uses `drizzle-orm/neon-http` for normal
// queries, that's fine — they're independent. Add the Pool here for the
// queue alone, or pass an existing `Pool` from your own setup.

import type {
	JobDefinition,
	JobMapFromDefinition,
	JobStore
} from '@absolutejs/queue';
import { Pool, neonConfig } from '@neondatabase/serverless';
import { drizzle } from 'drizzle-orm/neon-serverless';
import { buildPostgresJobStore } from './store';

// Re-export so consumers don't need to depend on @neondatabase/serverless
// directly just to set neonConfig (the bun + WebSocket polyfill dance).
export { neonConfig };

export type CreateNeonJobStoreOptions<Def extends JobDefinition> = {
	jobs: Def;
} & ({ connectionString: string } | { pool: Pool });

// Pass an existing Neon WebSocket `Pool` to share its connections, or a
// `connectionString` to let the adapter open its own pool. `jobs` is your
// `defineJobs` definition — it types and validates payloads.
export const createNeonJobStore = <const Def extends JobDefinition>(
	options: CreateNeonJobStoreOptions<Def>
): JobStore<JobMapFromDefinition<Def>> => {
	const pool =
		'pool' in options
			? options.pool
			: new Pool({ connectionString: options.connectionString });
	const db = drizzle({ client: pool });

	return buildPostgresJobStore(db, options.jobs);
};
