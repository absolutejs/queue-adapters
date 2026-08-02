# @absolutejs/queue-postgres

Postgres storage adapter for [`@absolutejs/queue`](https://www.npmjs.com/package/@absolutejs/queue), built on Drizzle. The
production `JobStore`: durable, with atomic multi-worker claiming via
`FOR UPDATE SKIP LOCKED`. Ships convenience factories for both `postgres.js`
and Neon's WebSocket driver (`@neondatabase/serverless`); the underlying
`buildPostgresJobStore` accepts any Drizzle Postgres database, so other
drivers (including Bun SQL and `node-postgres`) work too. Its portable JSONB
encoding preserves payload objects across all supported drivers.

## Install

```bash
# pick your driver
bun add @absolutejs/queue @absolutejs/queue-postgres drizzle-orm postgres
# …or
bun add @absolutejs/queue @absolutejs/queue-postgres drizzle-orm @neondatabase/serverless
```

`postgres` and `@neondatabase/serverless` are **optional** peer deps —
install whichever one you'll use.

## Usage with `postgres.js`

```ts
import { createPostgresJobStore } from '@absolutejs/queue-postgres/postgres';
import { createJobRegistry, defineJobs, queue, t } from '@absolutejs/queue';
import postgres from 'postgres';

const jobs = defineJobs({
	'email.send': t.Object({ to: t.String(), subject: t.String() })
});
const registry = createJobRegistry(jobs).on(
	'email.send',
	async ({ to, subject }) => {}
);

// Share your app's existing postgres.js client (one pool)…
const client = postgres(process.env.DATABASE_URL, { prepare: false });
const store = createPostgresJobStore({ client, jobs });

// …or let the adapter open its own connection:
// const store = createPostgresJobStore({ connectionString: url, jobs });

app.use(queue({ registry, store }));
```

## Usage with Neon (`@neondatabase/serverless`)

`createNeonJobStore` mirrors `createPostgresJobStore` but uses Neon's
WebSocket Pool. **Important**: the queue's `claimDue` opens a transaction
and selects with `FOR UPDATE SKIP LOCKED`. Neon's HTTP driver
(`drizzle-orm/neon-http`) is single-statement and can't do row-level
locks — use the WebSocket Pool driver here. Your app's other code can
keep using the HTTP driver; they're independent.

```ts
import {
	createNeonJobStore,
	neonConfig
} from '@absolutejs/queue-postgres/neon';
import { createJobRegistry, defineJobs, queue, t } from '@absolutejs/queue';
import { Pool } from '@neondatabase/serverless';

// Bun ships a global WebSocket; for node, polyfill once:
//   import ws from 'ws'; neonConfig.webSocketConstructor = ws;

const jobs = defineJobs({
	'email.send': t.Object({ to: t.String(), subject: t.String() })
});

// Share an existing Neon Pool…
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const store = createNeonJobStore({ jobs, pool });

// …or let the adapter open its own:
// const store = createNeonJobStore({ connectionString: url, jobs });
```

## Usage with any other Drizzle Postgres adapter

Both factories are thin wrappers around `buildPostgresJobStore(db, jobs)`,
which accepts any `PgDatabase<any, any, any>` from Drizzle. If you use
`drizzle-orm/node-postgres`, build the `db` yourself and pass it in:

```ts
import { buildPostgresJobStore, queueSchema } from '@absolutejs/queue-postgres';
import { drizzle } from 'drizzle-orm/node-postgres';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const db = drizzle(pool, { schema: queueSchema });
const store = buildPostgresJobStore(db, jobs);
```

### Migrations

Add the table to your Drizzle schema so it's included in migrations:

```ts
export { queueJobsTable } from '@absolutejs/queue-postgres';
```

Then `drizzle-kit generate` / `push` as usual.

## How claiming works

`claimDue` runs inside a transaction:

```sql
SELECT … FROM queue_jobs
WHERE status = 'pending' AND run_at <= $now
ORDER BY run_at LIMIT $n
FOR UPDATE SKIP LOCKED;        -- concurrent workers skip locked rows
-- then UPDATE those ids → status='claimed'
```

This guarantees a job is handed to exactly one worker even with many workers polling.
Crashed workers are recovered by `reapStuck` (lease expiry → back to `pending`).

## License

Apache-2.0 — see [LICENSE](./LICENSE).
