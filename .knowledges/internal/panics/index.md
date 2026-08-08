---
type: Unit
title: internal/panics
description: Shared panic recovery and panic-value-to-text conversion, kept its own package because it belongs to no layer.
---

# Responsibility

One implementation of recovering a panic and rendering its value as text. The
transport recovers panics from consume and payload-fill callbacks; the plugin
server recovers them from request decodes and from application handlers.

# Boundary

It is its own package precisely because those callers share nothing else. A copy
in each is how the guarantee drifts out of one of them, which is how the defect
it exists to prevent was introduced the first time.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- recovery and rendering: `internal/panics/panics.go`
