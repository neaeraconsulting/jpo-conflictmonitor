# Add `jpo-conflictmonitor-batch-processing` subproject for ATSPM/SPaT comparison

### Summary

Adds a new, independently deployed Spring Boot subproject, `jpo-conflictmonitor-batch-processing`, alongside the existing Kafka Streams application. It runs scheduled (batch) analyses that don't fit a streaming model. Specifically, it compares processed CV SPaT data against controller event logs pulled from an [ATSPM](https://github.com/udotdevelopment/ATSPM) server, to validate broadcast SPaT against an independent, non-CV source of truth, the signal controller's own event log.

Full background, rationale, architecture, and configuration details are in [`jpo-conflictmonitor-batch-processing/README.md`](jpo-conflictmonitor-batch-processing/README.md).

### What's included

- New Maven module (`jpo-conflictmonitor-batch-processing/`) with its own `pom.xml`, `Dockerfile`, `docker-compose-batch.yml`, and `sample.env`.
- Scheduled task per configured ATSPM route/signal that:
  - Queries ATSPM for controller event logs and reads the corresponding SPaT data already written to MongoDB by the core streams app.
  - Pairs SPaT indication changes to ATSPM controller events by signal-group to phase mapping, and computes per-indication (RED/YELLOW/GREEN) match percentages.
  - Persists comparison logs to MongoDB and raises events when pairing falls below 90%, or when SPaT signal groups and ATSPM phases don't align at all (likely misconfiguration).
- `TestController` (`/test/**`) for manually exercising the ATSPM client and internal services during development.  The test controller is configured to be disabled by default, and should only be enabled for local testing, not in production.
- Root `sample.env` and `docker-compose.yml` updated with the new `cm_batch` profile and required env vars.

### Notes

- This module has no Kafka dependency and doesn't touch the existing streams topology. The only integration point with the core app is reading the shared `ProcessedSpat` Mongo collection.
- ATSPM connectivity requires `CM_ATSPM_CLIENT_BASE_URL`/`_USERNAME`/`_PASSWORD`. See `sample.env`.

### Tests

Includes a comprehensive set of unit tests with > 80% coverage.
