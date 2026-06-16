import type {
	JobDefinition,
	JobMapFromDefinition,
	JobStore
} from '@absolutejs/queue';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { buildPostgresJobStore } from './store';

type PostgresClient = ReturnType<typeof postgres>;

export type CreatePostgresJobStoreOptions<Def extends JobDefinition> = {
	jobs: Def;
} & ({ client: PostgresClient } | { connectionString: string });

// Pass an existing postgres.js `client` to share its connection pool, or a
// `connectionString` to let the adapter open its own. `jobs` is your defineJobs
// definition — it types and validates payloads.
export const createPostgresJobStore = <const Def extends JobDefinition>(
	options: CreatePostgresJobStoreOptions<Def>
): JobStore<JobMapFromDefinition<Def>> => {
	const client =
		'client' in options
			? options.client
			: postgres(options.connectionString, { prepare: false });
	const db = drizzle({ client });

	return buildPostgresJobStore(db, options.jobs);
};
