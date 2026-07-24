#!/usr/bin/env bash
#
# bench-regime.sh runs the shared-memory benchmark's small-payload cells under one
# scheduler regime, for the scheduler-regime matrix. The benchmark labels every row
# with the regime and self-verifies that the regime is actually in effect
# (bench/shm's verifyRegime fatals otherwise), so a row can never be mislabeled.
#
#   gomaxprocs1  GOMAXPROCS=1 — single OS thread. No privileges.
#   gc-churn     forced GC between batches (STYX_SHM_GC_CHURN=1). No privileges.
#   cgroup2cpu   a finite cgroup v2 CPU quota. Needs user-scope cgroup delegation;
#                when the runner cannot install one, this SKIPS with an explicit
#                annotation rather than silently passing (an unquota'd run would be
#                a mislabeled cgroup2cpu row).
#
# The default-scheduling ("preemption churn") regime is intentionally not here: Go
# enables asynchronous preemption by default, and the only related knob,
# GODEBUG=asyncpreemptoff=1, DISABLES preemption rather than increasing churn — so
# there is no env knob that drives more churn than the default already does.
set -euo pipefail

regime="${1:?usage: bench-regime.sh <gomaxprocs1|gc-churn|cgroup2cpu>}"

# The matrix observes the shared-memory cells under load; a few repetitions are
# enough to surface a regime-specific pathology without a long run.
bench_args=(./bench/shm -run='^$'
  -bench='BenchmarkUnary/impl=(production-shm|production-shm-sync)/payload=64/concurrency=1'
  -benchmem -count=3)

case "$regime" in
  gomaxprocs1)
    GOMAXPROCS=1 STYX_SHM_REGIME=gomaxprocs1 go test "${bench_args[@]}"
    ;;
  gc-churn)
    STYX_SHM_GC_CHURN=1 STYX_SHM_REGIME=gc-churn go test "${bench_args[@]}"
    ;;
  cgroup2cpu)
    if ! command -v systemd-run >/dev/null 2>&1; then
      echo "::notice title=cgroup2cpu skipped::systemd-run unavailable; cannot install a user-scope CPU quota"
      exit 0
    fi
    # Probe: can this runner create a user-scope scope with a CPU quota at all?
    if ! systemd-run --user --scope -p CPUQuota=200% -- true >/dev/null 2>&1; then
      echo "::notice title=cgroup2cpu skipped::user-scope cgroup CPU delegation unavailable on this runner"
      exit 0
    fi
    systemd-run --user --scope -p CPUQuota=200% -- \
      env STYX_SHM_REGIME=cgroup2cpu go test "${bench_args[@]}"
    ;;
  *)
    echo "unknown regime: $regime" >&2
    exit 2
    ;;
esac
