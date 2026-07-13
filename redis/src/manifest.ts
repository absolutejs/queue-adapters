import { defineImplementation, defineManifest } from '@absolutejs/manifest';
import { Type } from '@sinclair/typebox';
import type { CreateRedisJobStoreOptions } from './index';

export const manifest = defineManifest<CreateRedisJobStoreOptions>()({
	contract: 1,
	identity: {
		accent: '#dc382d',
		category: 'infrastructure',
		description:
			'Redis-backed `JobStore` for `@absolutejs/queue`. Atomic claiming via Lua, sorted-set scheduling by `runAt`, per-job hash records, idempotency keys with SET NX. Takes any client satisfying the narrow `RedisCommandClient` surface — ioredis directly, node-redis v4+ via a small adapter.',
		docsUrl: 'https://github.com/absolutejs/queue-adapters/tree/main/redis',
		name: '@absolutejs/queue-redis',
		tagline: 'Keep background jobs in Redis for high-throughput queues.'
	},
	implements: [
		defineImplementation<CreateRedisJobStoreOptions>()({
			contract: 'queue/job-store',
			factory: 'createRedisJobStore',
			from: '@absolutejs/queue-redis',
			requires: {
				env: [
					{
						description: 'Redis connection URL',
						example: 'redis://default:pass@host:6379',
						key: 'REDIS_URL',
						secret: true
					}
				],
				peers: [
					{
						name: 'ioredis',
						range: '>= 5.0.0',
						reason: 'Redis driver (structurally satisfies RedisCommandClient)'
					}
				],
				services: [
					{
						description: 'Stores queued jobs',
						id: 'redis'
					}
				]
			},
			settings: Type.Object({
				keyPrefix: Type.Optional(
					Type.String({
						default: 'absolutejs:queue:',
						description:
							'Prefix for every Redis key the queue writes. Change it to isolate multiple apps or environments sharing one Redis.',
						title: 'Key prefix'
					})
				)
			}),
			title: 'Redis',
			wiring: {
				code: 'createRedisJobStore({ client: new Redis(${env.REDIS_URL} ?? "redis://127.0.0.1:6379"), keyPrefix: ${settings.keyPrefix} })',
				imports: [
					{
						from: '@absolutejs/queue-redis',
						names: ['createRedisJobStore']
					},
					{ from: 'ioredis', names: ['Redis'] }
				]
			}
		})
	],
	settings: Type.Object({}),
	wiring: []
});
