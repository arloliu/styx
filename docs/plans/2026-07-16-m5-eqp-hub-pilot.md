# eqp-hub Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status: outline-level plan** — to be refined into a full task-by-task plan when the preceding milestone completes.

**Goal:** Integrate Styx as a production-ready plugin transport for eqp-hub, migrate one low-risk device type, validate in staging, and deliver a go/no-go recommendation for broader rollout.

## Global Constraints

- Module `github.com/arloliu/styx`; Linux amd64 primary; pure Go.
- Validation before every commit: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- Never add Co-Authored-By or other attribution trailers to commits.
- eqp-hub's plugin package fetching stays host-side per Open Question 4 (Styx takes a path + optional hash, but does not fetch).
- Dual-path migration: tap_nats device type must support both go-plugin and Styx at runtime behind a feature flag; no forced transition.
- All integration work assumes the hardening work (fuzz/chaos/soak/benchmark gates all passing) is complete.

---

## Task Overview & Model Assignment

| Task | Model | Effort | Rationale |
|------|-------|--------|-----------|
| 1. Define eqp-hub device-plugin contract | sonnet | high | API compatibility judgment against a real consumer; lifecycle contract (Init/Start/Stop/HotReload/SaveRuntimeState/CollectMetrics/Ping) becomes an ordinary Styx protobuf service |
| 2. Host-side integration shim in eqp-hub | sonnet | high | Map eqp-hub supervisor expectations (restart policy, health, stderr→Fluent, error taxonomy) onto Styx Host/Events; error-mapping is outage-critical |
| 3. Migrate tap_nats device type | sonnet | high | Production migration with rollback story behind config flag; dual-path capable; one low-risk device to prove the pattern |
| 4. Pilot validation in staging | opus | high | Soak in eqp-hub staging, compare latency/CPU/restart behavior vs. go-plugin baseline, write pilot report with recommendation |

---

## Task 1: Define eqp-hub Device-Plugin Lifecycle Contract as a Styx Service

**Model/Effort/Why:** sonnet / high — Requires API-compatibility judgment against a real consumer. The lifecycle contract (Init/Start/Stop/HotReload/SaveRuntimeState/CollectMetrics/Ping) from eqp-hub's existing `arloliu/go-plugin` fork must map cleanly to a standard Styx protobuf service with generated stubs, working under eqp-hub's existing buf pipeline.

**Files:**
- `examples/eqp-hub/device_plugin.proto` — protobuf service definition for eqp-hub device-plugin contract
- `examples/eqp-hub/device_plugin_service.go` — type definitions and helper methods for the device-plugin service in Go
- `docs/eqp-hub-integration.md` — integration guide explaining the contract and how eqp-hub uses Styx for device plugins
- `/home/arlo/projects/eqp-hub/styx_plugin_adapter.go` — (in eqp-hub repo) shim that bridges eqp-hub's supervisor to Styx Host/ClientConn

**Acceptance Criteria:**
- Protobuf service definition includes all methods from eqp-hub's existing lifecycle contract: Init, Start, Stop, HotReload, SaveRuntimeState, CollectMetrics, Ping.
- Service compiles under eqp-hub's buf configuration; generated stubs work with protoc-gen-go-styx.
- Method signatures preserve eqp-hub's request/response shapes; no breaking changes to the logical interface.
- Contract document explains streaming methods (SaveRuntimeState returns a stream of state chunks; CollectMetrics returns a stream of metric events).
- All methods include proper error response handling with eqp-hub's error taxonomy (critical vs. transient).

**Steps:**
- [ ] Review eqp-hub's current lifecycle contract in `arloliu/go-plugin` fork; document all method signatures and semantics.
- [ ] Design protobuf service definition in `examples/eqp-hub/device_plugin.proto` mapping each lifecycle method; handle Init/Start/Stop as unary, SaveRuntimeState/CollectMetrics as streaming.
- [ ] Generate Go stubs via protoc-gen-go-styx; validate compilation under eqp-hub's buf pipeline.
- [ ] Define helper types (InitRequest/Response, StartRequest/Response, etc.) in `examples/eqp-hub/device_plugin_service.go`.
- [ ] Document error taxonomy mapping (eqp-hub critical/transient → Styx error types) in `docs/eqp-hub-integration.md`.
- [ ] Implement a mock device-plugin server in Go; verify handshake, method dispatch, and error handling.
- [ ] Commit: `feat(eqp-hub): define device-plugin lifecycle contract as Styx protobuf service`

---

## Task 2: Host-Side Integration Shim in eqp-hub

**Model/Effort/Why:** sonnet / high — Two real systems meeting: eqp-hub's supervisor and Styx's Host/Events API. The error-mapping decision (distinguishing critical device faults from transient failures) is outage-relevant; restart policy, health semantics, stderr routing, and metrics collection all require careful integration.

**Files:**
- `/home/arlo/projects/eqp-hub/styx_plugin_adapter.go` — adapter converting Styx Host/ClientConn/Events into eqp-hub's plugin supervisor expectations
- `/home/arlo/projects/eqp-hub/styx_error_mapper.go` — error-taxonomy mapper: (PluginCrashError, PluginPanicError, ErrOutcomeUnknown, etc.) → (critical/transient/unknown)
- `/home/arlo/projects/eqp-hub/styx_health_classifier.go` — health classifier using Styx heartbeat progress counters (wedged/overloaded/draining)
- `/home/arlo/projects/eqp-hub/styx_stderr_router.go` — capture plugin stderr, route to Fluent with device ID, severity, timestamps
- `/home/arlo/projects/eqp-hub/styx_integration_test.go` — integration tests: host adapter with mock Styx server, error mapping, health transitions

**Acceptance Criteria:**
- Styx Host/Events fully express eqp-hub's restart policy, health checks, and error handling requirements.
- Error mapping is explicit and documented: PluginCrashError=critical, ErrOutcomeUnknown=unknown-retryable, timeouts/backpressure=transient.
- Stderr capture integrates with eqp-hub's Fluent pipeline (tagged with device ID, severity, timestamp).
- Health classification (wedged/overloaded/draining) maps to eqp-hub's restart policies without false positives.
- Metrics from Styx heartbeats (progress counters, arena occupancy, in-flight count) feed into eqp-hub's observability stack.
- All integration points are tested: handshake, lifecycle transitions, error handling, health transitions, stdout/stderr routing.

**Steps:**
- [ ] Design adapter API: eqp-hub supervisor calls → Styx Host methods (Start, Stop, HotReload) with appropriate config.
- [ ] Implement `styx_plugin_adapter.go` wrapping Styx Host and translating eqp-hub's supervisor API to Host calls.
- [ ] Implement error mapper in `styx_error_mapper.go`: classify each Styx error type as critical/transient/unknown.
- [ ] Implement health classifier in `styx_health_classifier.go` using Styx heartbeat counters and state machine.
- [ ] Implement stderr router in `styx_stderr_router.go` capturing child stderr and routing to Fluent with device tags.
- [ ] Write integration tests in `styx_integration_test.go` exercising all adapter paths (normal operation, crashes, restarts, hot-reload).
- [ ] Validate error mapping against eqp-hub's critical-fault shutdown logic; ensure no misclassifications cause outages.
- [ ] Commit: `feat(eqp-hub): add Styx integration shim (adapter, error mapper, health classifier, stderr router)`

---

## Task 3: Migrate tap_nats Device Type Behind Config Flag

**Model/Effort/Why:** sonnet / high — Production migration with rollback capability. tap_nats is a low-risk device type (simple lifecycle, deterministic state, no complex cleanup); dual-path capability allows instant fallback to go-plugin if Styx fails. The migration pattern becomes a template for other device types.

**Files:**
- `/home/arlo/projects/eqp-hub/devices/tap_nats/tap_nats_styx_bridge.go` — new Styx-native implementation of tap_nats device-plugin contract
- `/home/arlo/projects/eqp-hub/devices/tap_nats/config.go` — extend to support `use_styx_transport: bool` config flag
- `/home/arlo/projects/eqp-hub/devices/tap_nats/supervisor.go` — dual-path logic: pick Styx or go-plugin based on flag
- `/home/arlo/projects/eqp-hub/devices/tap_nats/tap_nats_styx_test.go` — test both paths (Styx and go-plugin) side-by-side
- `/home/arlo/projects/eqp-hub/staging/tap_nats_canary.yaml` — canary deployment config: start with go-plugin, gradually shift to Styx

**Acceptance Criteria:**
- tap_nats compiles with `use_styx_transport: true` or `false`; both code paths are exercised in tests.
- Styx path implements full device-plugin contract: Init (load config, connect to NATS), Start (begin subscribing), Stop (graceful drain), HotReload (state save/restore), CollectMetrics, Ping.
- Dual-path logic is transparent to eqp-hub's supervisor: same lifecycle events, error mapping, and restart policies regardless of transport.
- Rollback is one-line config change; no data loss or service interruption on rollback.
- Staging canary config enables 0% Styx traffic initially, supports gradual ramp (10%, 50%, 100%).

**Steps:**
- [ ] Implement tap_nats Styx bridge in `tap_nats_styx_bridge.go`: device-plugin service methods calling native NATS SDK.
- [ ] Extend config in `config.go` to include `use_styx_transport: bool` with default false (safe-by-default).
- [ ] Implement dual-path supervisor logic in `supervisor.go`: instantiate Styx Host or legacy go-plugin based on flag.
- [ ] Write comprehensive tests in `tap_nats_styx_test.go`: both paths invoked in identical scenarios, outputs compared.
- [ ] Prepare staging canary config `tap_nats_canary.yaml` with traffic ramp controls.
- [ ] Run local smoke tests: tap_nats with Styx transport, verify NATS connectivity, state handoff during hot-reload.
- [ ] Document rollback procedure in `docs/tap-nats-migration.md`.
- [ ] Commit: `feat(tap_nats): migrate to Styx transport behind config flag (dual-path capable)`

---

## Task 4: Pilot Validation in eqp-hub Staging

**Model/Effort/Why:** opus / high — The judgment call that decides broader rollout. Requires soak testing in a real fab environment, capturing latency/CPU/restart behavior against the go-plugin baseline, identifying any stability or performance regressions, and synthesizing a go/no-go decision with documented rationale.

**Files:**
- `/home/arlo/projects/eqp-hub/staging/styx_pilot_harness.go` — test harness that submits load to tap_nats via both transports, captures metrics
- `/home/arlo/projects/eqp-hub/staging/styx_pilot_metrics.go` — metrics collector: latency histograms, CPU, memory, fd usage, restart counts
- `/home/arlo/projects/eqp-hub/staging/pilot_report_template.md` — template for pilot report (findings, comparison table, go/no-go recommendation)
- `docs/styx-pilot-validation-report.md` — final pilot report (filled from template, with recorded data and recommendation)

**Acceptance Criteria:**
- Soak runs for ≥24 hours in eqp-hub staging with tap_nats behind Styx and go-plugin running in parallel.
- Captured metrics: p50/p95/p99 latency, throughput, CPU usage (host + plugin), memory (RSS), fd count, restart events, timeout/crash counts.
- Comparison table documents delta vs. baseline: "Styx p50 +2% (2.8 µs vs. 2.7 µs), CPU -5% (8.2% vs. 8.6%), restarts +0".
- No regressions in critical dimensions (latency p99 not >10% worse, CPU not >20% worse, restart rate not elevated).
- Pilot report clearly states go/no-go and conditional checkpoints (e.g., "GO for production rollout; GO-SLOW for multi-device migration").
- Report is reviewed and approved by eqp-hub maintainers before recommendation is final.

**Steps:**
- [ ] Set up staging environment: tap_nats running with both Styx and go-plugin instances in parallel.
- [ ] Implement pilot harness in `styx_pilot_harness.go`: submit NATS subscriptions/publishes to both paths, compare results.
- [ ] Implement metrics collector in `styx_pilot_metrics.go`: capture latency, CPU, memory, fds, restarts from both paths.
- [ ] Run pilot soak for 24+ hours; collect metrics hourly.
- [ ] Analyze data: build comparison table (p50/p95/p99 latency, throughput, CPU, memory, restarts).
- [ ] Write pilot report using `pilot_report_template.md`: findings, comparison table, go/no-go decision, conditional checkpoints.
- [ ] Review report with eqp-hub maintainers; incorporate feedback.
- [ ] Publish approved report as `docs/styx-pilot-validation-report.md`.
- [ ] Commit: `docs(pilot): add styx pilot validation report (staging soak results, go/no-go recommendation)`
