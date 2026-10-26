## 0.1.9

- Accept `@absolutejs/queue` 0.9 and 0.10 as peers; the store's tests run against 0.10.1. 0.1.8's `<0.9.0` range kept consumers on queue 0.10 on 0.1.5, whose claims ignore the worker's handler kinds and dead-letter other workers' jobs.

## 0.1.8

- Add a driver-neutral `/drizzle` entry point for stores and schema exports without loading optional connection drivers.

# Changelog

## 0.1.7 — 2026-09-16

- Filter claims atomically by the store definition and the worker's registered handler kinds before applying limits and row locks.
- Advertise `supportsKindFiltering` so strict workers reject incompatible adapters at startup.
- Leave unsupported jobs pending for another worker, including when supported jobs sit behind them or the handler set is empty.

## 0.1.6 — 2026-09-16

- Fence completion and failure by claim token so expired workers cannot overwrite reclaimed jobs.
- Count expired leases against the retry budget and dead-letter exhausted jobs.
- Add the claim-token column to the exported queue schema. Existing databases must add this nullable column before enabling fenced workers.
- Validate PostgreSQL store behavior against queue 0.8.2.
