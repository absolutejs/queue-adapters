# @absolutejs/queue-redis

Redis-backed `JobStore` for
[@absolutejs/queue](https://github.com/absolutejs/queue). Sibling to
[`@absolutejs/queue-postgres`](../postgres) — same `JobStore`
contract, different transport.

## When to use Redis vs Postgres

| Concern | `queue-redis` | `queue-postgres` |
|---|---|---|
| Already have Redis | Win | New dep to operate |
| Already have Postgres | Skip — fewer deps | Win |
| Durability after Redis crash | RDB snapshots + AOF (config-dependent) | WAL — point-in-time recovery |
| Throughput | Higher per-key (in-memory) | Lower (transactional) |
| Multi-region | Native cluster geo-replication | PG logical replication (heavier) |
| At-most-once semantics | Lease-based reap-on-expire | Same |

Both adapters implement identical `JobStore` shape — swap them by
config.

## Install

```sh
bun add @absolutejs/queue @absolutejs/queue-redis
bun add ioredis      # OR
bun add redis        # node-redis v4+
```

## Usage with `ioredis`

```ts
import { Redis } from 'ioredis';
import { createQueueWorker, createJobRegistry, defineJobs } from '@absolutejs/queue';
import { createRedisJobStore } from '@absolutejs/queue-redis';

const redis = new Redis(process.env.REDIS_URL!);

const jobs = defineJobs({
  'email.send': t.Object({ to: t.String(), subject: t.String() }),
});

const store = createRedisJobStore<typeof jobs._inferred>({
  client: redis,                    // ioredis structurally satisfies RedisCommandClient
  keyPrefix: 'myapp:queue:',         // optional
});

const registry = createJobRegistry(jobs).on('email.send', async (payload) => {
  await sendEmail(payload);
});

const worker = createQueueWorker({ store, registry });
worker.start();
```

## Usage with `node-redis` v4+

```ts
import { createClient } from 'redis';

const client = createClient({ url: process.env.REDIS_URL });
await client.connect();

// node-redis's typed wrappers have slightly different signatures
// (e.g. `client.hSet(key, fields)` is camelCase). Adapt:
const adapted: RedisCommandClient = {
  hset: (key, fields) => client.hSet(key, fields),
  hgetall: (key) => client.hGetAll(key),
  hdel: (key, ...fields) => client.hDel(key, fields),
  del: (...keys) => client.del(keys),
  zadd: (key, score, member) => client.zAdd(key, { score, value: member }),
  zrem: (key, ...members) => client.zRem(key, members),
  zrangebyscore: (key, min, max, offset, count) =>
    client.zRangeByScore(key, min, max,
      offset !== undefined && count !== undefined
        ? { LIMIT: { offset, count } }
        : undefined),
  zcard: (key) => client.zCard(key),
  sadd: (key, ...m) => client.sAdd(key, m),
  srem: (key, ...m) => client.sRem(key, m),
  set: (key, value, mode) => client.set(key, value, mode === 'NX' ? { NX: true } : undefined),
  get: (key) => client.get(key),
  scard: (key) => client.sCard(key),
  smembers: (key) => client.sMembers(key),
  eval: (script, keys, args) => client.eval(script, { keys, arguments: args }),
};

const store = createRedisJobStore({ client: adapted });
```

## Atomic claim via Lua

`claimDue` runs a Lua script that atomically:

1. `ZRANGEBYSCORE due 0 now LIMIT 0 N` — find due jobs
2. `ZREM due id` for each — remove from the due set
3. `ZADD claimed (now+leaseMs) id` for each — add to claimed with expiry
4. `HSET <job> status=claimed lockedAt=now lockedBy=worker`
5. `HGETALL <job>` for each — return the payloads

Without Lua, two concurrent workers could race the ZRANGEBYSCORE → ZREM
gap and both think they own the same job. With Lua, the entire 5-step
sequence is one atomic operation — Redis runs Lua single-threaded.

`reapStuck` uses a sibling Lua script to find expired-lease claimed
jobs and move them back to due.

## Storage layout

All keys prefixed by `keyPrefix` (default `'absolutejs:queue:'`):

- `<prefix>job:<id>` — HASH per job (the record)
- `<prefix>due` — ZSET keyed by `runAt`
- `<prefix>claimed` — ZSET keyed by `lockedAt + leaseMs` (lease expiry)
- `<prefix>idempotency:<key>` — STRING mapping idempotency key → job id

## v0.0.1 surface

| Method | Status |
|---|---|
| `enqueue` (with idempotency) | ✓ |
| `claimDue` (atomic Lua) | ✓ |
| `complete` | ✓ |
| `fail` (with retry / dead-letter) | ✓ |
| `reapStuck` (atomic Lua) | ✓ |
| `get` | ✓ |
| `countByStatus` | partial — counts pending + claimed; v0.0.1 returns 0 for done/dead/canceled |
| `cancel`, `list`, `listByKind`, `retry` | deferred to 0.1.0 |

The deferred methods need a separate maintained index (`<prefix>byKind:<kind>`
SET, `<prefix>byStatus:<status>` SET) which would cost a write on every
status transition. v0.0.1 ships without to keep the hot path lean;
0.1.0 adds opt-in lazy indexing.

## Crash safety

Redis pub/sub-style queues are subject to your Redis durability
config:

- **AOF on, fsync every second**: at most 1s of work lost on Redis crash
- **AOF on, fsync always**: no work lost, slower
- **RDB only**: minutes of work lost — NOT recommended for queues

For at-most-once semantics, Redis durability is sufficient. For
at-least-once, the worker should mark a job complete/failed only AFTER
the side effect succeeds — `@absolutejs/queue`'s worker already does
this.

## License

[Apache 2.0](../LICENSE). Tier B substrate-adjacent — rides
`@absolutejs/queue` (BSL Tier A).
