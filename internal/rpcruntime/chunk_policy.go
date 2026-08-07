package rpcruntime

import (
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// ChunkPolicy is one connection's stream-chunking admission policy, resolved
// once at construction from the negotiated tuple, the attach-carried chunk
// ceiling, and the transport's own negotiated per-direction payload limits
// (stream-protocol.md §13.1/§13.6). Its zero value means chunking is
// inactive: Active is false, and the three uint32 fields carry no meaning
// when it is — every existing construction site that supplies no policy gets
// this zero value, unchanged behavior.
//
// Ceiling bounds the reassembled logical stream message on both directions
// (§13.6). SendInline and RecvInline are this connection's exact negotiated
// per-direction payload maxima — the outbound and inbound canonical fragment
// length the split rule is defined against (§13.2) — read off the concrete
// transport underneath the transport.Transport interface (raw shared memory,
// or the burst composite wrapping it). They differ from Ceiling, which bounds
// the whole reassembled message rather than one fragment, and from each
// other whenever the two directions' negotiated limits are asymmetric.
type ChunkPolicy struct {
	Active     bool
	Ceiling    uint32
	SendInline uint32
	RecvInline uint32
}

// StreamTableOption configures a StreamTable at construction. See
// WithChunkPolicy.
type StreamTableOption func(*StreamTable)

// WithChunkPolicy installs this connection's stream-chunking policy. Omitted,
// a StreamTable's policy is the zero ChunkPolicy — chunking inactive — which
// is what every construction site predating this option gets.
func WithChunkPolicy(p ChunkPolicy) StreamTableOption {
	return func(t *StreamTable) { t.chunkPolicy = p }
}

// ChunkPolicyFor resolves one connection's stream-chunking policy from
// whether the feature is active and tr's own negotiated per-direction
// payload limits. active is evaluated by the caller against its negotiated
// tuple and announced ceiling (control.ChunkingActive) — this package does
// not import internal/control, so the two host- and plugin-side callers each
// do that evaluation themselves and share only this resolution step.
//
// The base transport.Transport interface exposes no limit accessor of its
// own, so the two exact directional maxima are read off the concrete
// transport underneath it: the raw shared-memory transport's
// MaxSendPayload/MaxRecvPayload, or the burst composite's InlineMax/
// InboundInlineMax when the burst path also wraps this connection. The
// result is the zero (inactive) ChunkPolicy whenever active is false, or tr
// is a transport that never carries this frame kind (uds).
func ChunkPolicyFor(tr transport.Transport, active bool, ceiling uint32) ChunkPolicy {
	if !active {
		return ChunkPolicy{}
	}

	switch t := tr.(type) {
	case *shmtransport.Transport:
		return ChunkPolicy{
			Active: true, Ceiling: ceiling,
			SendInline: t.MaxSendPayload(), RecvInline: t.MaxRecvPayload(),
		}
	case *BurstTransport:
		return ChunkPolicy{
			Active: true, Ceiling: ceiling,
			SendInline: t.InlineMax(), RecvInline: t.InboundInlineMax(),
		}
	default:
		return ChunkPolicy{}
	}
}
