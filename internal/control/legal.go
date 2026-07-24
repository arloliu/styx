package control

import (
	"errors"
	"time"

	"github.com/arloliu/styx/internal/control/controlpb"
)

// MessageKind identifies a ControlMessage's oneof case without requiring
// a type switch at every call site.
type MessageKind int

const (
	KindHello MessageKind = iota
	KindHelloAck
	KindAttachRegion
	KindAttachRegionAck
	KindHeartbeat
	KindHeartbeatAck
	KindDrain
	KindDrainAck
	KindResume
	KindResumeAck
	KindSaveState
	KindSaveStateAck
	KindShutdown
	KindShutdownAck
	KindPoisoned
	KindRestore
	KindRestoreAck
)

// LifecycleState tracks the coarse control-plane state each side is in to decide
// which message kinds are legal to receive.
type LifecycleState int

const (
	StateHandshaking LifecycleState = iota
	StateAttaching
	StateServing
	StateDraining
	StateShuttingDown
	// StateRestoring is the freshly spawned successor's state during a hot-reload's
	// restore-and-validate phase.
	// It has finished handshake and attach but is not yet promoted.
	// Only Restore/RestoreAck traffic is legal; a Restore in StateServing would indicate
	// the host tried to reset an already-promoted instance's state underneath live calls.
	StateRestoring
)

// ReplyDeadlines maps each message kind to the deadline for its reply to arrive.
// Drain and Shutdown carry their own deadline_unix_nano field for the phase itself; this is the deadline for the reply.
// Ack kinds and Resume/Poisoned are absent — they are replies or arrive unsolicited, so they have no reply deadline.
//
//exhaustive:ignore -- only kinds that expect a reply appear here.
var ReplyDeadlines = map[MessageKind]time.Duration{
	KindHello:        2 * time.Second,
	KindAttachRegion: 2 * time.Second,
	KindHeartbeat:    500 * time.Millisecond,
	KindDrain:        30 * time.Second,
	KindSaveState:    10 * time.Second,
	KindShutdown:     5 * time.Second,
}

// ErrProtocolViolation is returned for any control-protocol contract
// breach: MSG_TRUNC/MSG_CTRUNC, a message exceeding MaxMessageSize, a
// message kind illegal for the current LifecycleState, or an fd-count
// mismatch.
var ErrProtocolViolation = errors.New("control: protocol violation")

// legalByState is the per-lifecycle-state table of legal message kinds.
// It defines which message kinds are valid in each lifecycle state.
var legalByState = map[LifecycleState]map[MessageKind]bool{
	StateHandshaking: {
		KindHello:    true,
		KindHelloAck: true,
	},
	StateAttaching: {
		KindAttachRegion:    true,
		KindAttachRegionAck: true,
	},
	StateServing: {
		KindHeartbeat:    true,
		KindHeartbeatAck: true,
		KindDrain:        true,
		KindSaveState:    true,
		KindSaveStateAck: true,
		KindShutdown:     true,
		KindPoisoned:     true,
	},
	StateDraining: {
		KindDrainAck:     true,
		KindResume:       true,
		KindResumeAck:    true,
		KindSaveState:    true,
		KindSaveStateAck: true,
		KindShutdown:     true,
		KindPoisoned:     true,
	},
	StateShuttingDown: {
		KindShutdownAck: true,
		KindPoisoned:    true,
	},
	StateRestoring: {
		KindRestore:    true,
		KindRestoreAck: true,
	},
}

// KindOf returns msg's MessageKind by inspecting the oneof, or (0, false)
// if msg.Body is unset (itself a protocol violation the caller must reject).
func KindOf(msg *controlpb.ControlMessage) (MessageKind, bool) {
	switch msg.GetBody().(type) {
	case *controlpb.ControlMessage_Hello:
		return KindHello, true
	case *controlpb.ControlMessage_HelloAck:
		return KindHelloAck, true
	case *controlpb.ControlMessage_AttachRegion:
		return KindAttachRegion, true
	case *controlpb.ControlMessage_AttachRegionAck:
		return KindAttachRegionAck, true
	case *controlpb.ControlMessage_Heartbeat:
		return KindHeartbeat, true
	case *controlpb.ControlMessage_HeartbeatAck:
		return KindHeartbeatAck, true
	case *controlpb.ControlMessage_Drain:
		return KindDrain, true
	case *controlpb.ControlMessage_DrainAck:
		return KindDrainAck, true
	case *controlpb.ControlMessage_Resume:
		return KindResume, true
	case *controlpb.ControlMessage_ResumeAck:
		return KindResumeAck, true
	case *controlpb.ControlMessage_SaveState:
		return KindSaveState, true
	case *controlpb.ControlMessage_SaveStateAck:
		return KindSaveStateAck, true
	case *controlpb.ControlMessage_Shutdown:
		return KindShutdown, true
	case *controlpb.ControlMessage_ShutdownAck:
		return KindShutdownAck, true
	case *controlpb.ControlMessage_Poisoned:
		return KindPoisoned, true
	case *controlpb.ControlMessage_Restore:
		return KindRestore, true
	case *controlpb.ControlMessage_RestoreAck:
		return KindRestoreAck, true
	default:
		return 0, false
	}
}

// Legal reports whether kind is a legal message to receive while in state,
// per the per-lifecycle-state table above. Anything not in the table is
// illegal — the caller treats a false result as ErrProtocolViolation.
func Legal(state LifecycleState, kind MessageKind) bool {
	return legalByState[state][kind]
}
