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
