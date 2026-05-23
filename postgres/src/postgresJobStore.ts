import type { JobMap, JobStore } from '@absolutejs/queue';
import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { queueSchema } from './schema';
import { buildPostgresJobStore } from './store';

type PostgresClient = ReturnType<typeof postgres>;

export type CreatePostgresJobStoreOptions =
	| { client: PostgresClient }
	| { connectionString: string };

// Pass an existing postgres.js `client` to share its connection pool, or a
// `connectionString` to let the adapter open its own.
export const createPostgresJobStore = <Jobs extends JobMap>(
	options: CreatePostgresJobStoreOptions
): JobStore<Jobs> => {
	const client =
		'client' in options
			? options.client
			: postgres(options.connectionString, { prepare: false });
	const db = drizzle(client, { schema: queueSchema });

	return buildPostgresJobStore<Jobs>(db);
};
