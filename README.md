# @absolutejs/queue-adapters

Storage adapters for [`@absolutejs/queue`](https://github.com/absolutejs/queue) — a
private workspace monorepo. Each adapter is published as its own package and depends
on `@absolutejs/queue`; install only the one for your backend.

| Adapter | Package | Backend | Status |
| --- | --- | --- | --- |
| `postgres/` | `@absolutejs/queue-postgres` | Postgres (Drizzle, `FOR UPDATE SKIP LOCKED`) | ✅ |
| `sqlite/` | `@absolutejs/queue-sqlite` | SQLite | planned |
| `redis/` | `@absolutejs/queue-redis` | Redis | planned |

## Why a monorepo

Adapters share the `JobStore` contract from `@absolutejs/queue`. Keeping them in one
workspace means a contract change can update every adapter in a single PR, with one CI
and one `bun install` — while each still publishes independently so consumers pull only
the backend (and its heavy deps) they actually use.

## Develop

```bash
bun install          # installs every workspace member
bun run typecheck    # across all adapters
bun run test         # across all adapters
bun run build        # across all adapters
```

> Each adapter currently dev-links the core via `file:../../queue`. Once
> `@absolutejs/queue` is published, switch these to the published version (the model
> `@absolutejs/voice-adapters` uses).

## License

CC BY-NC 4.0
