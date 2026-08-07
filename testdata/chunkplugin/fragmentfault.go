//go:build failpoint

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
)

// fragmentFaultEnv names one fragment-acceptance boundary of this instance's own
// chunked sends and the error to fail it with, as
// "<phase>:<fragment index>:<action>" — for example
// "before-admission:8:closed". The phase is one of the two boundary names
// rpcruntime publishes; the index is the fragment's 0-based position in the
// train; the action selects the injected error (see faultActions).
//
// It exists because the plugin-to-host half of stream-protocol.md §13.8 fails in
// the PLUGIN's process: the seam that selects which fragment fails, and where in
// that fragment's acceptance it fails, is process-wide, so only the process
// running the train can arm it. A test builds this fixture with -tags failpoint
// and names the boundary here.
const fragmentFaultEnv = "STYX_CHUNK_FRAGMENT_FAULT"

// fragmentFaultInjected is the line the fixture appends at the instant the fault
// fires, carrying the boundary it fired at. Without it a test could only observe
// the send failing, not that it failed at the boundary it selected.
const fragmentFaultInjected = "chunkplugin: fragment fault injected"

// faultActions maps an action name to the error injected at the boundary. Each
// stands for one shape of stream-protocol.md §13.8: a failing connection
// (closed, poisoned) or reject-mode queue-full (backpressure).
var faultActions = map[string]error{
	"closed":       transport.ErrClosed,
	"poisoned":     transport.ErrPoisoned,
	"backpressure": transport.ErrBackpressure,
}

// installFragmentFault arms the fragment-boundary seam from fragmentFaultEnv, if
// a test named one. The fault fires at most ONCE per process: a test that drives
// traffic over the same instance after the abandoned train needs the successor
// sends to be clean, so a second train reaching the same boundary passes through
// untouched.
//
// Every other fixture knob is untouched by this, and an unset (or unparsable)
// variable installs nothing at all.
func installFragmentFault() {
	phase, index, injected, ok := parseFragmentFault(os.Getenv(fragmentFaultEnv))
	if !ok {
		return
	}

	var fired atomic.Bool
	rpcruntime.SetChunkFragmentFailpoint(func(p rpcruntime.ChunkFragmentPoint) error {
		if p.Phase != phase || p.Index != index || !fired.CompareAndSwap(false, true) {
			return nil
		}
		report(fmt.Sprintf("%s %s at fragment %d", fragmentFaultInjected, phase, index))

		return injected
	})
}

// parseFragmentFault reads fragmentFaultEnv's three fields, reporting ok=false
// for anything it cannot read as a known phase, a non-negative index, and a
// known action. A malformed value arms nothing rather than arming something the
// test did not ask for.
func parseFragmentFault(spec string) (phase string, index int, injected error, ok bool) {
	fields := strings.Split(spec, ":")
	if len(fields) != 3 {
		return "", 0, nil, false
	}

	phase = fields[0]
	if phase != rpcruntime.ChunkFragmentBeforeAdmission && phase != rpcruntime.ChunkFragmentAfterAccept {
		return "", 0, nil, false
	}

	index, err := strconv.Atoi(fields[1])
	if err != nil || index < 0 {
		return "", 0, nil, false
	}

	injected, known := faultActions[fields[2]]
	if !known {
		return "", 0, nil, false
	}

	return phase, index, fmt.Errorf("chunkplugin: injected fragment fault: %w", injected), true
}
