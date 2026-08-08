---
type: Unit
title: supervisor
description: The public RestartPolicy configuration surface a host supplies to control crash restarts.
---

# Responsibility

The restart policy types a host configures: `RestartPolicy` (attempt cap and
backoff), `BackoffFunc`, and the `NoRestart` zero value under which styx never
respawns a crashed plugin on its own.

# Boundary

Configuration only — it executes nothing. `internal/supervisor` reads these
values and runs the restart loop, emits the event stream, and captures crash
reasons. Do not confuse the two packages: this one is public and
dependency-light, that one is internal and does the work.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the policy type: `supervisor/policy.go` → `RestartPolicy`
- the never-restart zero value: `supervisor/policy.go` → `NoRestart`
