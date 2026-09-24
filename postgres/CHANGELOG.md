## 0.1.7

- Add a driver-neutral `/drizzle` entry point for stores and schema exports without loading optional connection drivers.

# Changelog

## 0.1.6 — 2026-09-16

- Fence completion and failure by claim token so expired workers cannot overwrite reclaimed jobs.
- Count expired leases against the retry budget and dead-letter exhausted jobs.
- Add the claim-token column to the exported queue schema. Existing databases must add this nullable column before enabling fenced workers.
- Validate PostgreSQL store behavior against queue 0.8.2.
