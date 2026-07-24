import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';

/* No serializable top-level config: both factories take a connection (from
 * env) plus the app's `jobs` definition — the module-scope binding declared
 * by @absolutejs/queue's default wiring recipe, which these snippets
 * reference by name. */
export const manifest = defineManifest<Record<never, never>>()({
	contract: 2,
	identity: {
		accent: '#336791',
		category: 'infrastructure',
		description:
			'Postgres `JobStore` for `@absolutejs/queue`, built on Drizzle. Atomic multi-worker claiming via `FOR UPDATE SKIP LOCKED`, crashed-worker recovery through lease reaping, portable JSONB across Bun SQL, postgres.js and Neon, and convenience factories for driver-owned pools — or bring any Drizzle Postgres database.',
		docsUrl:
			'https://github.com/absolutejs/queue-adapters/tree/main/postgres',
		name: '@absolutejs/queue-postgres',
		tagline: 'Keep background jobs safe in your Postgres database.'
	},
	implements: [
		defineImplementation<never>()({
			contract: 'queue/job-store',
			factory: 'createPostgresJobStore',
			from: '@absolutejs/queue-postgres/postgres',
			requires: {
				env: [
					{
						description:
							'Postgres connection string (the queue_jobs table lives here)',
						example: 'postgres://user:pass@host/db',
						key: 'DATABASE_URL',
						secret: true
					}
				],
				peers: [
					{
						name: 'drizzle-orm',
						range: '>= 1.0.0-rc.1',
						reason: 'query builder the store is built on'
					},
					{
						name: 'postgres',
						range: '>= 3.4.0',
						reason: 'postgres.js driver'
					}
				],
				services: [
					{
						description: 'Stores queued jobs durably',
						id: 'postgres'
					}
				]
			},
			title: 'Postgres (postgres.js driver)',
			wiring: {
				// `jobs` is the module-scope defineJobs binding declared by
				// @absolutejs/queue's default wiring recipe.
				code: 'createPostgresJobStore({ connectionString: ${env.DATABASE_URL} ?? "", jobs })',
				imports: [
					{
						from: '@absolutejs/queue-postgres/postgres',
						names: ['createPostgresJobStore']
					}
				]
			}
		}),
		defineImplementation<never>()({
			contract: 'queue/job-store',
			factory: 'createNeonJobStore',
			from: '@absolutejs/queue-postgres/neon',
			requires: {
				env: [
					{
						description:
							'Neon Postgres connection string (the queue_jobs table lives here)',
						docsUrl: 'https://console.neon.tech',
						example: 'postgres://user:pass@host/db',
						key: 'DATABASE_URL',
						secret: true
					}
				],
				peers: [
					{
						name: 'drizzle-orm',
						range: '>= 1.0.0-rc.1',
						reason: 'query builder the store is built on'
					},
					{
						name: '@neondatabase/serverless',
						range: '>= 0.10.0',
						reason: 'Neon WebSocket driver (transactions + row locks)'
					}
				],
				services: [
					{
						description: 'Stores queued jobs durably',
						id: 'postgres'
					}
				]
			},
			title: 'Neon serverless Postgres (WebSocket driver)',
			wiring: {
				code: 'createNeonJobStore({ connectionString: ${env.DATABASE_URL} ?? "", jobs })',
				imports: [
					{
						from: '@absolutejs/queue-postgres/neon',
						names: ['createNeonJobStore']
					}
				]
			}
		})
	],
	lifecycle: [
		{
			docsUrl:
				'https://github.com/absolutejs/queue-adapters/tree/main/postgres#migrations',
			id: 'migrate',
			idempotent: true,
			kind: 'migration',
			title: 'Create the queue_jobs table (re-export queueJobsTable from your Drizzle schema, then generate/push)',
			when: 'before-first-run'
		}
	],
	settings: Type.Object({}),
	wiring: []
});
