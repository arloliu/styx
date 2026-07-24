// Command hot-reload-plugin is examples/hot-reload's plugin binary: a stateful
// Echo server that survives a hot-reload with its state intact. Each Say response
// is prefixed with the number of calls this plugin has served ("<count>:<msg>"),
// and the call count is sealed into the reload snapshot by SaveState and read back
// by RestoreState — so after the host hot-reloads the plugin, the freshly spawned
// successor resumes counting from where the predecessor left off instead of
// resetting to zero. It reuses examples/echo's generated Echo service, so no new
// protobuf is needed to demonstrate real state handoff.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// counterSnapshotVersion is the framework's current snapshot envelope
// version. The framework stamps it on every snapshot (SaveState supplies no
// version of its own); RestoreState validates it so a plugin never misreads
// an envelope from a different framework version.
const counterSnapshotVersion uint32 = 1

// counterServer answers Echo's Say, prefixing each response with the running
// count of calls this instance has served. The count is the state that a
// hot-reload must carry across to the successor instance.
type counterServer struct{ calls atomic.Uint64 }

func (s *counterServer) Say(_ context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	n := s.calls.Add(1)

	return &echopb.SayResponse{Message: strconv.FormatUint(n, 10) + ":" + req.GetMessage()}, nil
}

// counterState registers the plugin's reload state handoff over the same counter.
// SaveState runs on the predecessor once it has drained and frozen (its snapshot
// is sealed and verified by the host); RestoreState runs on the freshly spawned
// successor before it begins serving, so the successor resumes from the saved
// count. RestoreState validates the format version and payload it is handed
// rather than trusting them (see counterSnapshotVersion).
type counterState struct{ srv *counterServer }

func (c counterState) SaveState(context.Context) ([]byte, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, c.srv.calls.Load())

	return buf, nil
}

func (c counterState) RestoreState(_ context.Context, formatVersion uint32, data []byte) error {
	if formatVersion != counterSnapshotVersion {
		return fmt.Errorf("hot-reload plugin: unknown snapshot format version %d (want %d)",
			formatVersion, counterSnapshotVersion)
	}
	if len(data) != 8 {
		return fmt.Errorf("hot-reload plugin: malformed snapshot: %d bytes, want 8", len(data))
	}
	c.srv.calls.Store(binary.LittleEndian.Uint64(data))

	return nil
}

func main() {
	srv := styx.NewPluginServer(styx.PluginServerConfig{})

	impl := &counterServer{}
	echopb.RegisterEchoServer(srv, impl)

	state := counterState{srv: impl}
	srv.RegisterStateSaver(state)
	srv.RegisterStateRestorer(state)

	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
