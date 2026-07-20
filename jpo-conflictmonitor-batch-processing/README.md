# jpo-conflictmonitor-batch-processing

## Background

The rest of this repository, `jpo-conflictmonitor`, is a real-time **Kafka Streams**
application: it consumes SPaT, MAP and BSM messages as they arrive, validates them against
one another, and publishes conflict/event data to Kafka topics (which are in turn sunk to
MongoDB via Kafka Connect, see `jpo-utils`). That model — react to a continuous stream of
messages as they arrive — fits most of the Conflict Monitor's analyses well, but it doesn't
fit every analysis.

This subproject, `jpo-conflictmonitor-batch-processing`, is a separate Spring Boot
application for analyses that are inherently **batch/pull-based** rather than
stream/push-based, and don't belong in the Kafka Streams topology. It runs as its own
process/container, with its own `pom.xml`, `Dockerfile` and `docker-compose-batch.yml`, and
is versioned and deployed independently of the streams application. It shares the MongoDB
instance (and some data model classes, via the `jpo-geojsonconverter` dependency) with the
main application, but does not participate in the Kafka topology at all.

## Purpose: ATSPM/SPaT comparison

[ATSPM](https://github.com/udotdevelopment/ATSPM) (Automated Traffic Signal Performance
Measures) is an open source traffic signal performance measurement system originally
developed by UDOT. It has a client/server architecture: a backend service ingests high
resolution controller event logs from signal controllers (phase changes, detector
actuations, etc.) and exposes them over an authenticated HTTP API. Many agencies already
operate an ATSPM instance independently of, and prior to, deploying the Connected Vehicle
(CV) infrastructure that produces SPaT/MAP/BSM messages.

Because ATSPM records signal phase/indication changes directly from the traffic
controller, its event log is a good independent source of ground truth for what a signal
was actually doing at a given point in time. This project's core feature —
**ATSPM/SPaT comparison** — periodically:

1. Queries the ATSPM API for controller event logs (phase begin-green/yellow/red-clearance
   events, etc.) for a configured set of intersections ("signals" in ATSPM's data model,
   organized into "routes"), for a recent time window.
2. Reads the corresponding SPaT data that the main `jpo-conflictmonitor` streams
   application has already processed and stored in MongoDB (`ProcessedSpat` collection,
   sourced from broadcast CV SPaT messages) for the same intersections and time window.
3. Maps the SPaT message's `signalGroupId` to the ATSPM controller's phase number (this
   mapping is intersection-specific and configured per-signal, since it is not implied by
   either data source alone; a phase may also have a "secondary" phase for skipped/
   overlapping phase scenarios).
4. Pairs each SPaT signal indication change with the nearest matching ATSPM controller
   event within a small time window, and computes the percentage of SPaT indications
   (broken out by RED/YELLOW/GREEN) that could be matched to a corresponding ATSPM event.
5. Persists the raw comparison ("pair") results to MongoDB, and raises an event
   (`AtspmSpatPairEvent`) when the paired percentage for any indication color drops below
   90%, which is used as a signal that the broadcast SPaT data may be diverging from what
   the controller is actually doing (e.g. misconfigured phase mapping, RSU/controller
   connectivity issues, or bugs in map/SPaT generation).
6. Also raises an `AtspmSpatSignalGroupAlignmentEvent` when the set of `signalGroupId`s
   seen in the SPaT data doesn't map onto the set of phases seen in the ATSPM data at all,
   which usually indicates a configuration mismatch (e.g. missing/incorrect signal group to
   phase mapping in the `application.yaml`) rather than a runtime signal problem.

In short: the Kafka Streams application validates CV messages against *each other*; this
batch application validates the CV SPaT messages against an *independent, non-CV source of
truth* (the physical controller's own event log, via ATSPM). It is intended to catch
misconfiguration and long-running drift issues that intra-CV-message validation can't see.

## Why a separate application, and why batch/scheduled rather than streamed

- **Pull vs. push:** ATSPM is a request/response HTTP API, not a stream. There is no
  Kafka topic of ATSPM events to consume from; the data has to be actively queried for
  a time range, which is naturally a scheduled/batch operation rather than a Kafka Streams
  topology.
- **Different cadence:** ATSPM controller event logs and CV SPaT data don't need to be
  compared in real time — the comparison is useful on the order of once per hour (see
  `interval`/`interval-units` below), not per-message. Running it as a scheduled task
  avoids adding latency-sensitive, low-value load to the streaming pipeline.
- **Independent lifecycle:** ATSPM is a separate, optional, third-party system. Not every
  deployment of `jpo-conflictmonitor` has an ATSPM instance available. Keeping this as a
  separate Spring Boot service means it can be deployed (or not) independently via its own
  Docker Compose profile (`cm_batch`), without affecting or being a hard dependency of the
  core streams application.
- **Different data flow direction:** the core application's job is to consume and validate
  the live CV message stream. This application instead pulls historical data from two
  external stores (ATSPM's API and the CM's own Mongo output) after the fact and produces
  a comparison — a fundamentally batch/ETL-style workflow, for which Spring's
  `@Scheduled`/`TaskScheduler` model is a more natural fit than Kafka Streams.

## Architecture

This service talks to two external data sources and one shared datastore. It calls the
ATSPM server's HTTP API directly to pull controller event logs. It reads SPaT data out of
the `ProcessedSpat` MongoDB collection, which it does not write — that collection is
populated by the core `jpo-conflictmonitor` Kafka Streams application (via a Kafka Connect
Mongo sink connector, run as part of the shared `jpo-utils` infrastructure) from broadcast
CV SPaT messages. This service then writes its own comparison results and events back into
that same MongoDB instance, into its own set of collections (all prefixed `Cm`, e.g.
`CmAtspmSpatPairLog`, `CmAtspmSpatPairEvent`, `CmAtspmSpatSignalGroupAlignmentEvent`) so as
not to collide with the core application's collections. MongoDB is the only integration
point between this service and the core streams application — there is no direct
dependency or Kafka topic shared between them.

### Key packages

- `algorithms` — Interfaces/base classes for the scheduled-task "algorithm" abstraction
  used by this project (`Algorithm`, `ConfigurableAlgorithm`, `ExecutableAlgorithm`,
  `ScheduledTaskAlgorithm`). Modularity is implemented with a service-locator pattern
  (`AtspmSpatValidationAlgorithmFactory`) so that alternate implementations of the
  comparison algorithm can be swapped in via configuration
  (`cm.batch.atspm.spat.validation.algorithm`) without code changes.
- `algorithms/atspm_spat_validation` — Configuration/parameters for the ATSPM/SPaT
  comparison algorithm: routes, signals, phase mappings, scheduling interval, grace
  period, etc. (bound from `application.yaml` via `AtspmSpatValidationParameters`).
- `scheduler` — `SchedulerConfig`/`ScheduledTaskErrorHandler` configure the shared
  `ThreadPoolTaskScheduler` used by all algorithms, including its error handler (an
  uncaught exception in a scheduled task is logged and swallowed, not left to silently
  cancel that task's future executions).
- `scheduler/atspm_spat_validation` — Spring `TaskScheduler`-based implementation that
  runs one recurring task per configured route (`AtspmSpatValidationTask`), staggered
  (`task-start-time-stagger`) so as not to hit the ATSPM server with simultaneous
  requests for every route at once.
- `services/atspm` — HTTP client for the ATSPM API (`AtspmClientService`), plus token
  acquisition/refresh (`AtspmTokenService`) for ATSPM's OAuth-style password grant.
- `services/spat` — Reads previously-processed SPaT data that the core streams
  application already wrote to MongoDB (`ProcessedSpatService`), and reshapes it into
  per-signal-group indication timelines for comparison.
- `services/atspm_spat_validation` — The actual pairing/comparison logic
  (`AtspmSpatValidationService`): matches SPaT indications to ATSPM controller events and
  computes per-signal-group / per-indication match statistics.
- `models/atspm/raw` — POJOs mirroring the ATSPM API's own data model, ported from the
  [ATSPM project's C# model classes](https://github.com/udotdevelopment/ATSPM/tree/master/AtspmApi/Models).
- `models/atspm/processed` — Derived/processed forms of the raw ATSPM data (e.g. events
  indexed by phase, for efficient time-window lookups).
- `models/atspm_spat` — The comparison result models (`AtspmSpatPair`,
  `AtspmSpatPairLog`, per-signal-group statistics) persisted to Mongo.
- `models/spat` — Simplified SPaT/signal-group-indication models derived from
  `ProcessedSpat`, used only within this project's comparison logic.
- `events` — Mongo-persisted event documents (`AtspmSpatPairEvent`,
  `AtspmSpatSignalGroupAlignmentEvent`) raised when the comparison finds a problem.
- `mongo` — `ProcessedSpatCollectionUpdater`, which the scheduled task runs before each
  comparison to ensure the `ProcessedSpat` collection (written by the core application,
  whose `utcTimeStamp` field is stored as a string) has a proper indexed BSON `Date` field
  (`utcTimeStampTS`) so this project can run efficient time-range queries against it. The
  top-level `mongo/` directory has the equivalent queries as standalone `mongosh` scripts:
  `ProcessedSpat_AddTimestamp.js` mirrors the current in-place-update approach;
  `ProcessedSpat_MV.js` is an earlier, now-superseded approach that instead built a separate
  `ProcessedSpat_MV` materialized-view collection (see the "Add timestamp field to processed
  spat instead of using materialized view" commit).
- `controllers` — `ConflictMonitorBatchProcessingController` starts the configured
  scheduled algorithm(s) once the Spring context has finished loading. It runs as an
  `ApplicationRunner` (not from a bean constructor) so that loading the context - e.g. in
  tests - doesn't by itself start live scheduled tasks against real external services; it
  can be disabled entirely via `cm.batch.scheduler.enabled: false`. `TestController`
  exposes read-only `/test/**` HTTP endpoints that pass through to the ATSPM client and
  internal services, for manual testing/debugging (see `http-tests/`) — it is not part of
  the production data flow. These endpoints are unauthenticated and `/test/token` returns
  the live ATSPM access token, so the controller is **disabled by default** and must be
  explicitly enabled via `cm.batch.test-controller.enabled: true`
  (`CM_TEST_CONTROLLER_ENABLED=true`) — only do this for local development/testing, never
  in a shared or production environment.
- `time` — `ClockConfig` provides the application's `Clock` bean, which can optionally be
  offset to a fixed start time (`cm.batch.clock.offset` / `start-timestamp`) instead of
  system time. This is useful for re-running the comparison over a historical time window
  (e.g. to backfill or to reproduce/debug an incident) without waiting for real time to
  pass.

## Configuration

Configuration lives in `src/main/resources/application.yaml` under the `cm.batch` prefix,
plus environment variables (see `sample.env`). Key settings:

- `cm.batch.atspm.spat.validation.client.*` — ATSPM base URL and credentials
  (`CM_ATSPM_CLIENT_BASE_URL`, `CM_ATSPM_CLIENT_USERNAME`, `CM_ATSPM_CLIENT_PASSWORD`).
- `cm.batch.atspm.spat.validation.local-time-zone` — ATSPM's API expects/returns local
  (non-UTC) timestamps for the deployment's time zone; this is used to convert to/from UTC
  `Instant`s used everywhere else in the app.
- `cm.batch.atspm.spat.validation.interval` / `interval-units` — How often each route's
  comparison task runs (e.g. every 1 hour).
- `cm.batch.atspm.spat.validation.task-start-time-stagger` / `-units` — Offsets each
  route's task start time so that queries to the ATSPM server aren't all fired at once.
- `cm.batch.atspm.spat.validation.grace-period-offset` / `-units` — How far behind
  "now" to query, to allow time for data to be fully ingested into both ATSPM and the
  CM's own Mongo store before comparing. Not recommended to set to zero, since very recent
  data may not have arrived yet in one or both sources.
- `cm.batch.atspm.spat.validation.routes` — The list of routes/signals/intersections to
  monitor, and for each signal, the mapping from SPaT `signal-group-id` to ATSPM
  `primary-phase` (and, where applicable, `secondary-phase` for phases with skip/overlap
  behavior). A signal can be `enabled: false` to configure it (e.g. document why it's
  disabled) without actively querying it — e.g. if MAP or ATSPM data isn't yet available
  for that intersection.
- `cm.batch.clock.*` — See `time` package description above.
- `cm.batch.test-controller.enabled` (`CM_TEST_CONTROLLER_ENABLED`) — Enables
  `TestController`'s `/test/**` debug endpoints. **Disabled by default** — these endpoints
  are unauthenticated and `/test/token` returns the live ATSPM access token, so only
  enable this for local development/testing, never in a shared or production environment.
- Standard Spring Data MongoDB properties (`CM_DATABASE_NAME`, `DB_HOST_IP`, `MONGO_PORT`,
  `MONGO_READ_WRITE_USER`, `MONGO_READ_WRITE_PASS`, `CM_MONGO_AUTH_DB`) — this app connects
  to the same MongoDB database as the rest of `jpo-conflictmonitor`.

## Running

From the repository root:

```bash
docker compose -f docker-compose-batch.yml up --build -d
```

This brings up MongoDB (via the `jpo-utils` compose include) and the batch processing
service. It requires the same GitHub Maven credentials
(`MAVEN_GITHUB_TOKEN_NAME`/`MAVEN_GITHUB_TOKEN`/`MAVEN_GITHUB_ORG`) as the root project, to
pull the `jpo-geojsonconverter` dependency, plus `CM_ATSPM_CLIENT_BASE_URL`,
`CM_ATSPM_CLIENT_USERNAME` and `CM_ATSPM_CLIENT_PASSWORD` for the ATSPM API — see
`sample.env` (copy/merge into the root `.env`) and
`jpo-conflictmonitor-batch-processing/sample.env`.

It can also be enabled as part of a full-stack run of the root `docker-compose.yml` via the
`cm_batch` Compose profile (also included in `all`/`cm_full`/`cm_release`).

To run locally (outside Docker) for development, e.g. against a local ATSPM instance:

```bash
export CM_ATSPM_CLIENT_BASE_URL=...
export CM_ATSPM_CLIENT_USERNAME=...
export CM_ATSPM_CLIENT_PASSWORD=...
./mvnw spring-boot:run
```

See `http-tests/README.md` for manually exercising the `/test/**` endpoints (via the
IntelliJ HTTP Client or VSCode REST Client) against a running instance, useful for
verifying ATSPM connectivity/credentials and inspecting intermediate data independent of
the scheduled task.
