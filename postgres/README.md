# @absolutejs/queue-postgres

Postgres storage adapter for [`@absolutejs/queue`](../queue), built on Drizzle. The
production `JobStore`: durable, with atomic multi-worker claiming via
`FOR UPDATE SKIP LOCKED`.

## Install

```bash
bun add @absolutejs/queue @absolutejs/queue-postgres drizzle-orm postgres
```

## Usage

```ts
import { createPostgresJobStore } from '@absolutejs/queue-postgres';
import { createJobRegistry, defineJobs, queue, t } from '@absolutejs/queue';
import postgres from 'postgres';

// Define jobs once (kind -> payload schema); pass it to both the registry and
// the store. Types are inferred; payloads are validated.
const jobs = defineJobs({
	'email.recap': t.Object({ accountId: t.String() })
});
const registry = createJobRegistry(jobs).on(
	'email.recap',
	async ({ accountId }) => {}
);

// Share your app's existing postgres.js client (one pool)…
const client = postgres(process.env.DATABASE_URL, { prepare: false });
const store = createPostgresJobStore({ client, jobs });

// …or let the adapter open its own connection:
// const store = createPostgresJobStore({ connectionString: url, jobs });

app.use(queue({ registry, store }));
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

CC BY-NC 4.0
