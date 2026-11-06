# Architecture Decision Records

Decision records for the fleet telemetry portal. One file, one record per
decision that merits it: stack, architecture, or contract changes with a
lasting why. Small routine choices stay in code comments, PR descriptions, or
the feature doc.

| ADR     | Title                                                    | Status   | Date       |
| ------- | -------------------------------------------------------- | -------- | ---------- |
| ADR-001 | Stack, topology and delivery of the fleet telemetry MVP   | Accepted | 2026-11-05 |
| ADR-002 | Ingestion contract and delivery guarantees                | Accepted | 2026-11-05 |
| ADR-003 | Device identity and offline sync contract                 | Accepted | 2026-11-05 |
| ADR-004 | Failure boundaries: circuit breakers where they matter    | Accepted | 2026-11-05 |
| ADR-005 | Agent architecture: direct function calling with evals    | Accepted | 2026-11-05 |

Diagram of the resulting system: `docs/architecture.excalidraw`
(preview in `docs/architecture-preview.png`).

## ADR-001: Stack, topology and delivery of the fleet telemetry MVP

Status: Accepted — 2026-11-05

### Context

A corporate fleet monitoring portal (Specs S1–S10 of
`odd/tasks/fleet-telemetry.md`): asynchronous ingestion of vehicle telemetry,
specialized persistence for high-frequency series, resilience between
services, an AI agent over fleet state, a reactive web portal, an offline-first
driver app, load and chaos tests, and infrastructure as code. It is a learning
project with no deadline and a single machine for local execution.

The baseline platform of the posting company is React/Next, React Native, Go,
C#, Kafka, TimescaleDB, Cassandra, Druid and AWS, but the assignment explicitly
allows choosing the stack; what gets evaluated is the architectural reasoning
("demostrando que la IA funciona como tu exoesqueleto y no como una muleta").

### Decisions

- **Go monorepo with five binaries** (S1, S3). `ingest` (HTTP intake),
  `consumer` (bus → storage), `alerts` (rules over state), `agent` (LLM + tools),
  `gateway` (REST + SSE). One module, one build, five deployable units. The only
  synchronous inter-service call is `gateway → agent`; everything else couples
  through the bus. Go because concurrency, a small runtime image and a single
  static binary matter more here than framework ergonomics.
- **Redpanda as the broker, Kafka API** (S1). One binary, no Zookeeper, small
  enough for a laptop, and the Go client speaks the standard Kafka protocol, so
  the local choice does not leak into the design. Topic naming and consumer
  groups are Kafka-shaped on purpose.
- **TimescaleDB as the only datastore** (S2). Hypertables, native compression
  per chunk, continuous aggregates and retention policies on an extension of
  PostgreSQL. Cassandra was rejected: the operational surface (cluster sizing,
  repair, query-first data modeling) is not justified at this volume, and
  Postgres gives relational queries over devices and trips in the same
  dependency. Justification is part of the deliverable (S2).
- **Redis pub/sub for the real-time fan-out** (S5). Consumers publish the last
  processed event; gateways subscribe and push it through SSE. It is a bus for
  "latest", not a datastore: Timescale remains the durable record.
- **SSE instead of WebSockets** (S5). The flow is server→client only; SSE is
  plain HTTP, proxy-friendly (`proxy_buffering off`) and reconnects natively in
  the browser. WebSockets would add a protocol and statefulness we do not need.
- **Docker Compose for local execution, Terraform documented and not applied**
  (S9). The assignment explicitly accepts executive judgment when the machine
  cannot run the whole stack. Compose is the runnable path; Terraform is
  evidence of the infrastructure decision, not deployed infrastructure.
- **Contract-first**: the event schema, topic names and HTTP routes live in one
  shared package (`internal/contracts`) and are the single source both the
  mobile app and the backend code against.

### Consequences and trade-offs

- **Redis is not durable.** A gateway that starts after an event misses it, so
  the portal's first paint must load state from Timescale over REST and treat
  SSE as deltas only. This constraint is load-bearing for the portal design
  (T4/T5).
- **One module, five binaries** means the Go module boundary does not enforce
  service isolation; only imports and review discipline do. Accepted: the
  alternative (five modules) costs more than it buys for one developer.
- **Terraform not applied** means the IaC is unverified against a real account;
  it is design evidence, and the README says so.
- **Single-node Redpanda locally** has no replication, so partition and
  rebalance behavior differs from a cluster. Consumer-group semantics and the
  at-least-once path are still exercised (ADR-002).
- **Postgres is a single point of failure** in this topology. Scaling out means
  read replicas and retention/compression tuning before any sharding.

### References

- `odd/tasks/fleet-telemetry.md` (Specs S1, S2, S3, S5, S9; tasks T2–T6, T11)
- `docs/architecture.excalidraw` (topology), `deploy/docker-compose.yml`
- ADR-002 (what the bus guarantees), ADR-005 (the agent's place in the topology)

## ADR-002: Ingestion contract and delivery guarantees

Status: Accepted — 2026-11-05

### Context

Thousands of devices emit telemetry continuously, the broker delivers
at-least-once, and mobile clients retry after network failures. The assignment
asks for load tests that inject 10% duplicate requests and 5% errors (S8), which
makes duplicates a designed-for input rather than an accident. Two questions
have to be answered together: what does the system promise about an accepted
event, and how is a duplicate recognized.

### Decisions

- **At-least-once end to end; the consumer is idempotent, not exactly-once.**
  Exactly-once across a broker and a database requires distributed transactions
  and buys nothing here, because dedup is cheap and deterministic.
- **Three timestamps, and only one of them belongs to the identity.**
  `captured_at` is stamped by the device and is immutable; `received_at` is
  stamped by ingest when the batch is accepted; `stored_at` is the database
  clock at insert. A retry re-stamps `received_at` and `stored_at` but keeps
  `captured_at`, so a dedup key containing the intake or storage time would
  treat a replay as a new event.
- **Dedup identity is `UNIQUE(device_id, captured_at, seq)`.** A hypertable
  unique index must contain the partitioning column, and `time` is
  `captured_at`; including it is not a workaround but the only form that both
  satisfies Timescale and stays stable across retries.
- **Every event carries `seq`, monotonic per device, and an immutable
  device-stamped `ts`.** A retried event is a byte-identical replay: same
  `seq`, same `ts`, same payload. Identity of an event is
  `(device_id, seq)`; `ts` makes it storable as a time-series key.
- **Partition key is `device_id`.** All events of one device land in the same
  partition, so per-device ordering survives the bus and the consumer can use
  ordering without global coordination.
- **`POST /v1/telemetry` accepts a batch, not a single event, and answers
  `202 Accepted`.** The response means "durably queued", never "persisted".
  There is no per-event HTTP acknowledgment, because the only thing that can
  safely free client storage is the watermark (ADR-003).
- **The consumer never discards an event by content.** Equal coordinates are
  not duplication; only a repeated `(device_id, seq)` is. Volume is controlled
  at the source (adaptive frequency, ADR-003) and by Timescale's native
  compression, never by filtering in the pipeline.
- **Micro-batches everywhere**: intake and persistence accumulate and flush
  every 2–5s or N events, whichever comes first. There is no separate
  single-event path to maintain.
- **W3C `traceparent` travels with every event**, generated by the device and
  propagated into broker headers and logs, so one trace spans device → ingest →
  consumer → storage.

### Consequences and trade-offs

- **Device clocks are untrusted input.** `captured_at` is data, not system
  truth: a device with a skewed clock writes into an older chunk, and a
  retention policy could age those rows out sooner. Each timestamp has one
  reader — alerts use `captured_at` (the vehicle's own timeline), pipeline
  latency uses `received_at - captured_at`, and retention and health use
  `stored_at`.
- **`202` without per-event acknowledgment** means a client can only trust the
  watermark. Any code path that treats a `202` as "safe to delete" reintroduces
  the data-loss bug this contract exists to prevent.
- **Never discarding duplicates** costs storage proportional to client retries;
  compression and a retention policy are the answer, and both are cheap.
- **Per-device ordering** means a busy device cannot be parallelized across
  partitions, which bounds single-vehicle throughput. Acceptable: the fan-out
  is across devices, not within one.

### References

- `odd/tasks/fleet-telemetry.md` (Specs S1, S2, S8; task T3)
- ADR-001 (broker and store), ADR-003 (watermark and identity)
- `docs/architecture.excalidraw` (write path)

## ADR-003: Device identity and offline sync contract

Status: Accepted — 2026-11-05

### Context

The driver app must keep collecting coordinates with no network, persist them
locally (S6) and synchronize in bulk on reconnect. That creates a contract the
server has to honor: the client needs to know exactly which local rows are
already durable so it can delete them. The app also has to distinguish a vehicle
that is stopped (engine on, queue empty) from a vehicle that lost signal, which
is the difference between a dashboard and a support incident.

### Decisions

- **`device_id` is the installation identity, minted at
  `POST /v1/devices/register`; `driver_id` is the business identity** of the
  person. Reinstalling the app creates a new `device_id`, which starts at
  watermark 0 — no stale state can be inherited.
- **Registration and the watermark endpoint land in later slices, on purpose.**
  The slice that builds persistence authenticates with a static development
  JWT whose `sub` is the device id, and proves the watermark algorithm and its
  transaction without exposing an endpoint; `POST /v1/devices/register` lands
  with the mobile app (T8), where the installation identity is actually
  minted, and `GET /v1/devices/self/watermark` lands with the gateway (T4),
  which is the only place the read belongs.
- **JWT with a ~90 day lifetime, validated stateless.** At thousands of devices
  emitting every few seconds, a session lookup per request is the wrong
  bottleneck; rotation happens at registration.
- **The watermark is cumulative and FIFO.** `W` is the largest `seq` such that
  every event `1..W` for that device is durable. The app deletes local rows with
  `seq <= W` and nothing else. "Highest seq received" is forbidden: with
  out-of-order arrival or a lost batch it would delete events the server never
  persisted.
- **Absence of a watermark row means 0**, so a freshly registered device needs
  no special case. The value is monotonic and is advanced in the same
  transaction that persists the events.
- **`GET /v1/devices/self/watermark` (gateway)** is the only read the sync loop
  needs; it is authenticated by the same device JWT. Until it exists, the
  watermark is an internal contract proven by persistence tests.
- **Adaptive frequency**: ~2–5s while moving, a ~60s heartbeat while stopped
  (a ~30x reduction), and three derived states — moving, stopped (heartbeat
  alive), no signal (expected beat missed).

### Consequences and trade-offs

- **The heartbeat is both liveness and a data-reduction mechanism.** Without it
  "stopped" and "no signal" are indistinguishable; with it, a stationary
  vehicle costs ~60 events/hour instead of ~1800.
- **The watermark is the last completed run, not the largest complete prefix.**
  If `seq` 2 is missing while 3..9 are stored, the value stays where it was
  (0 for a fresh device) until the gap is filled and the run from `watermark+1`
  completes. Deliberately conservative: it never claims a durability the
  database does not have, at the cost of the device holding a few extra rows.
- **A `seq` gap blocks the watermark until the client resends.** That is
  correct and visible by design; it is also why the local queue must be
  append-only and drained in order.
- **A reinstall drops unsent events** along with the old `device_id`. Accepted
  for v1; the alternative is server-side identity recovery, which is a product
  decision this MVP does not need.
- **The watermark must never run ahead of durability.** Advancing it outside
  the persistence transaction turns a bug into silent client-side data loss.
- **Long-lived JWTs cannot be revoked individually** without a denylist. For
  v1, device deregistration is the revocation path.

### References

- `odd/tasks/fleet-telemetry.md` (Specs S6; tasks T3, T4, T8)
- ADR-002 (event identity and delivery guarantees)
- `docs/architecture.excalidraw` (watermark loop, adaptive frequency note)

## ADR-004: Failure boundaries: circuit breakers where they matter

Status: Accepted — 2026-11-05

### Context

The assignment asks for circuit breakers between microservices (S3). Taken
literally that suggests breaking every hop, but most hops here are asynchronous:
the broker already buffers, and a consumer that dies simply gets redelivered
work. A circuit breaker on a queue consumer is decoration.

### Decisions

- **Circuit breakers only where a synchronous dependency can block a user:
  `gateway → agent` and `agent → LLM`** (`gobreaker`, ADR-005 for the client).
  Those are the paths where a hung dependency would freeze the chat while
  everything else keeps working.
- **No circuit breaker on the asynchronous path.** The broker absorbs bursts;
  the consumer acknowledges only after a durable write, so a failure means
  redelivery and dedup makes redelivery safe (ADR-002).
- **Every outbound call carries a timeout**, and the breaker opens on repeated
  failures rather than on a single error, so the chat degrades fast instead of
  hanging.
- **Graceful shutdown per binary**: stop accepting, drain the in-flight batch,
  then exit; health endpoints report readiness, not just liveness.

### Consequences and trade-offs

- **The agent degrades, the portal does not.** When the LLM is down the chat
  answers with an explicit degradation message while maps and alerts keep
  working — that asymmetry is the point of scoping the breakers this narrowly.
- **A breaker without a timeout is not protection.** Both are configured
  together at the client construction site; a bare `gobreaker` around a
  default-timeout HTTP client would leave the failure mode open.
- **Deferred:** bulkheads, retries with jitter on the agent, and per-tool
  timeouts are out of v1 scope; the chat is a single user-facing feature, not a
  high-fanout API.

### References

- `odd/tasks/fleet-telemetry.md` (Specs S3; task T6)
- ADR-001 (topology), ADR-005 (the agent's LLM client)
- `docs/architecture.excalidraw` (CB markers on the synchronous paths)

## ADR-005: Agent architecture: direct function calling with evals

Status: Accepted — 2026-11-05

### Context

S4 asks for an agent integrated in the backend that consumes current vehicle
state and answers natural-language questions such as "¿qué vehículos llevan
detenidos más de 20 minutos en zonas críticas?". The obvious route is a
framework (LangChain, Semantic Kernel). The risk is the opposite of the
assignment's intent: a thick dependency that hides the loop, is hard to audit,
and turns an architectural demonstration into glue code.

### Decisions

- **The function-calling loop is written directly in Go against the LLM API.**
  No agent framework. The loop — send messages, receive a tool call, execute,
  return the result, repeat until a final answer — is a few dozen lines and
  stays fully readable and auditable.
- **8–10 read-only tools, one responsibility each**, covering fleet state
  (current positions, stopped vehicles, stopped-in-critical-zone, vehicle
  history, active alerts) plus `salud_pipeline()` for pipeline health. The agent
  never writes: no tool mutates state.
- **Tools are the security boundary.** The LLM can only reach what a tool
  exposes, and each tool is a bounded query with its own limits (time range, row
  cap). Prompt-level restrictions are not a security control.
- **Evals live offline in Python**, run over recorded sessions with a fixed
  question set, entirely separate from the runtime binary. Quality is measured
  by eval runs, not by inspecting transcripts in production.
- **The agent is reached only through `gateway`**, behind the breaker of
  ADR-004.

### Consequences and trade-offs

- **We own retry, streaming and tool-loop plumbing.** Accepted: it is small,
  and every behavior stays visible instead of framework-mediated.
- **Tool-schema or prompt changes need an eval run before merge**, which is real
  work per change; in exchange, regressions in answer quality are measurable
  rather than anecdotal.
- **No memory across conversations in v1**: each question is answered from live
  state, which keeps the agent stateless and cacheable but limits follow-ups.
- **Token cost and latency are per request**; the breaker and timeouts bound
  the blast radius, and the tool cap bounds the number of round trips.

### References

- `odd/tasks/fleet-telemetry.md` (Specs S4; task T6, evals in T6)
- ADR-004 (breaker and timeout on the LLM client)
- `docs/architecture.excalidraw` (agent, tools, external LLM)
