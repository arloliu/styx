package control_test

import (
	"fmt"
	"testing"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/stretchr/testify/require"
)

// allKinds enumerates every MessageKind so the table-driven test below can
// assert both the legal and illegal cases for each (state, kind) pair.
var allKinds = map[string]control.MessageKind{
	"Hello":           control.KindHello,
	"HelloAck":        control.KindHelloAck,
	"AttachRegion":    control.KindAttachRegion,
	"AttachRegionAck": control.KindAttachRegionAck,
	"Heartbeat":       control.KindHeartbeat,
	"HeartbeatAck":    control.KindHeartbeatAck,
	"Drain":           control.KindDrain,
	"DrainAck":        control.KindDrainAck,
	"Resume":          control.KindResume,
	"ResumeAck":       control.KindResumeAck,
	"SaveState":       control.KindSaveState,
	"SaveStateAck":    control.KindSaveStateAck,
	"Shutdown":        control.KindShutdown,
	"ShutdownAck":     control.KindShutdownAck,
	"Poisoned":        control.KindPoisoned,
}

// legalKindsByState is the expected legal set per the per-lifecycle-state
// table (mirrored independently from legal.go's doc comment, not copied
// from its implementation, so the test can't pass by tautology).
var legalKindsByState = map[string][]string{
	"Handshaking":  {"Hello", "HelloAck"},
	"Attaching":    {"AttachRegion", "AttachRegionAck"},
	"Serving":      {"Heartbeat", "HeartbeatAck", "Drain", "SaveState", "SaveStateAck", "Shutdown", "Poisoned"},
	"Draining":     {"DrainAck", "Resume", "ResumeAck", "SaveState", "SaveStateAck", "Shutdown", "Poisoned"},
	"ShuttingDown": {"ShutdownAck", "Poisoned"},
}

var allStates = map[string]control.LifecycleState{
	"Handshaking":  control.StateHandshaking,
	"Attaching":    control.StateAttaching,
	"Serving":      control.StateServing,
	"Draining":     control.StateDraining,
	"ShuttingDown": control.StateShuttingDown,
}

// Test Legal reporting the exact per-lifecycle-state legal-message table,
// exhaustively over every (LifecycleState, MessageKind) pair
func TestLegal_ReportsPerStateTable_ForEveryStateKindPair(t *testing.T) {
	for stateName, state := range allStates {
		t.Run(stateName, func(t *testing.T) {
			want := make(map[string]bool, len(legalKindsByState[stateName]))
			for _, kindName := range legalKindsByState[stateName] {
				want[kindName] = true
			}

			for kindName, kind := range allKinds {
				t.Run(kindName, func(t *testing.T) {
					// Given
					wantLegal := want[kindName]

					// When
					got := control.Legal(state, kind)

					// Then
					require.Equal(t, wantLegal, got, fmt.Sprintf("Legal(%s, %s)", stateName, kindName))
				})
			}
		})
	}
}

// newMessageForKind constructs a ControlMessage carrying kind's oneof case
// with a zero-value payload, used by TestKindOf_IdentifiesOneofCase to build
// one message per kind. The oneof wrapper types (ControlMessage_Hello, ...)
// are exported, but the Body field's interface type is not, so the whole
// ControlMessage literal is built here rather than returning a bare wrapper.
func newMessageForKind(kind control.MessageKind) *controlpb.ControlMessage {
	switch kind {
	case control.KindHello:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Hello{Hello: &controlpb.Hello{}}}
	case control.KindHelloAck:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_HelloAck{HelloAck: &controlpb.HelloAck{}}}
	case control.KindAttachRegion:
		return &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{}},
		}
	case control.KindAttachRegionAck:
		return &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_AttachRegionAck{AttachRegionAck: &controlpb.AttachRegionAck{}},
		}
	case control.KindHeartbeat:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: &controlpb.Heartbeat{}}}
	case control.KindHeartbeatAck:
		return &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_HeartbeatAck{HeartbeatAck: &controlpb.HeartbeatAck{}},
		}
	case control.KindDrain:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{}}}
	case control.KindDrainAck:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_DrainAck{DrainAck: &controlpb.DrainAck{}}}
	case control.KindResume:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Resume{Resume: &controlpb.Resume{}}}
	case control.KindResumeAck:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_ResumeAck{ResumeAck: &controlpb.ResumeAck{}}}
	case control.KindSaveState:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_SaveState{SaveState: &controlpb.SaveState{}}}
	case control.KindSaveStateAck:
		return &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_SaveStateAck{SaveStateAck: &controlpb.SaveStateAck{}},
		}
	case control.KindShutdown:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Shutdown{Shutdown: &controlpb.Shutdown{}}}
	case control.KindShutdownAck:
		return &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_ShutdownAck{ShutdownAck: &controlpb.ShutdownAck{}},
		}
	case control.KindPoisoned:
		return &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Poisoned{Poisoned: &controlpb.Poisoned{}}}
	default:
		return &controlpb.ControlMessage{}
	}
}

// Test KindOf identifying every oneof case, and reporting (0, false) when unset
func TestKindOf_IdentifiesOneofCase_ForEveryKind(t *testing.T) {
	for kindName, kind := range allKinds {
		t.Run(kindName, func(t *testing.T) {
			// Given
			msg := newMessageForKind(kind)

			// When
			got, ok := control.KindOf(msg)

			// Then
			require.True(t, ok)
			require.Equal(t, kind, got)
		})
	}

	t.Run("Unset", func(t *testing.T) {
		// Given
		msg := &controlpb.ControlMessage{}

		// When
		_, ok := control.KindOf(msg)

		// Then
		require.False(t, ok)
	})
}
