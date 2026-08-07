package styx

import (
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
)

// buildChunkPolicy resolves one connection's stream-chunking policy from the
// negotiated tuple, the attach-carried chunk ceiling, and tr's own negotiated
// per-direction payload limits (stream-protocol.md §13.1/§13.6). It is the
// zero (inactive) rpcruntime.ChunkPolicy whenever control.ChunkingActive is
// false, or tr is a transport that never carries this frame kind (uds) — the
// two conditions that leave the feature dormant on this connection.
//
// internal/supervisor's own chunkPolicyFor evaluates control.ChunkingActive
// the same way for the host side; both share the transport-inspecting half
// of the resolution in rpcruntime.ChunkPolicyFor.
func buildChunkPolicy(tr transport.Transport, tuple control.Tuple, chunkMaxPayload uint32) rpcruntime.ChunkPolicy {
	return rpcruntime.ChunkPolicyFor(tr, control.ChunkingActive(tuple, chunkMaxPayload), chunkMaxPayload)
}
