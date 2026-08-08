---
type: Unit
title: codec
description: The Codec and SizedMarshaler interfaces for RPC payload and Status encoding, with protobuf as the default.
---

# Responsibility

Defines the encoding seam the handshake negotiates: `Codec` for marshaling and
unmarshaling RPC payloads and `Status`, and `SizedMarshaler` for codecs that can
report an exact encoded size and marshal into a caller-supplied buffer so a
writer allocates its destination once.

# Boundary

Does not decide *when* to encode or where the bytes go — the runtime and
transport layers own that. Does not negotiate the codec; the handshake in
`internal/control` does, and this package only supplies the interface it
selects.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the encoding seam: `codec/codec.go` → `Codec`
- the sized fast path: `codec/codec.go` → `SizedMarshaler`
