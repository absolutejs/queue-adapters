# Changelog

## [0.0.1] — 2026-05-30

Initial preview. Redis-backed `JobStore` for `@absolutejs/queue`.

### Surface

- **`createRedisJobStore({ client, keyPrefix? })`** — returns
  `JobStore<Jobs>`. Pass any Redis client matching the narrow
  `RedisCommandClient` interface (ioredis structurally satisfies it
  directly; node-redis v4+ needs a thin adapter — see README).
- **Atomic `claimDue` via Lua.** Single-script atomic dequeue-mark-
  return — no race window between ZRANGEBYSCORE and ZREM. Two
  concurrent workers can't claim the same job.
- **Atomic `reapStuck` via Lua.** Finds claimed jobs whose lease has
  expired and moves them back to due in one operation.
- **Idempotency**: `enqueue` with `idempotencyKey` does a SET NX —
  duplicate enqueues return the existing job id.
- **Customizable `keyPrefix`** isolates multiple stores on one Redis.

### v0.0.1 method coverage

Required (`JobStore` contract):

- `enqueue` ✓
- `claimDue` ✓ (atomic Lua)
- `complete` ✓
- `fail` ✓
- `reapStuck` ✓ (atomic Lua)

Optional:

- `get` ✓
- `countByStatus` ✓ (partial — counts `pending` + `claimed`; v0.0.1
  returns 0 for `done`/`dead`/`canceled`. 0.1.0 will add opt-in lazy
  status indexes.)
- `cancel`, `list`, `listByKind`, `retry` — deferred to 0.1.0.

### Tested

12 tests against a mock Redis (with a small in-process Lua interpreter
for the two scripts the adapter ships): enqueue + HASH/ZSET shape;
claimDue moves jobs + reports them; claim respects limit; future-runAt
not claimed early; complete removes from claimed + due; fail with
retryAt restores; fail with dead doesn't requeue; reapStuck moves
expired-lease back to due; reapStuck reports 0 when no expiry;
idempotency returns existing id; countByStatus counts pending +
claimed; custom keyPrefix isolation.

### License

Apache 2.0 (Tier B substrate-adjacent — rides `@absolutejs/queue`
Tier A).
