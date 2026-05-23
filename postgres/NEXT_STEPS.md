# Next steps — @absolutejs/queue-postgres

Shipped (v0.0.1):
- `queueJobsTable` Drizzle schema + indexes (due, idempotency).
- `buildPostgresJobStore(db)` — driver-agnostic core (any Drizzle Postgres db).
- `createPostgresJobStore({ client | connectionString })` — postgres.js entry.
- `claimDue` via `FOR UPDATE SKIP LOCKED` in a transaction; `reapStuck` crash recovery.
- Integration tests run against **PGlite** (real Postgres in-process) — `bun test`.

Before publish:
- Restore `@absolutejs/queue` to `peerDependencies` (it's a devDependency `file:../queue`
  for local dev only — kept out of peers so `bun install` doesn't try to fetch the
  unpublished package from the registry).

Hardening / follow-ups:
- **Partial unique index** on `idempotency_key WHERE status IN ('pending','claimed')`
  as a hard race guard (today: select-then-insert covers the common case).
- **Real concurrency test** — PGlite is single-connection, so it validates the SQL but
  not true cross-worker contention. Add a docker-postgres integration test (N parallel
  workers, assert each job claimed once), mirroring `absolute-rag-postgresql`'s
  `scripts/smoke-docker.mjs`.
- **`LISTEN/NOTIFY` wakeup** — optional, to cut claim latency below the poll interval.
- **`@absolutejs/queue-sqlite`** sibling adapter for single-instance / edge.
- **Retention** — a `purgeDone({ olderThanMs })` helper to trim completed/dead rows.
