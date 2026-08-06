// Command burstplugin is the plugin fixture for the burst path's end-to-end
// tests: it echoes a Blob request's payload back unchanged whatever its size, so
// a caller can drive a payload far larger than the shared-memory arena can carry,
// and it records how its serving session ended.
//
// That record is the point of the fixture. A host tearing an instance down
// gracefully must leave the plugin exiting zero, and the only thing that tells
// that apart from an instance whose data plane died under it is what Serve
// returned. It is appended to the file named by STYX_BURST_EXIT_FILE rather than
// written to stderr, because a host's teardown stops delivering a plugin's stdio
// before the plugin has finished exiting — a file outlives the process either way.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// exitFileEnv names the file each instance appends its one exit line to,
// udsOnlyEnv makes this fixture advertise only the uds transport, and pidFileEnv
// names a file each instance appends its own process id to as it starts.
//
// The process id is what lets a test signal THIS instance. A host does not
// publish the process ids of the plugins it supervises, and a test that guessed
// at one — by scanning for the fixture's path, say — would be signalling whichever
// generation it happened to find rather than the one it is holding in a known
// state. Every instance appends, so the file is a generation-ordered record.
const (
	exitFileEnv = "STYX_BURST_EXIT_FILE"
	udsOnlyEnv  = "STYX_BURST_UDS_ONLY"
	pidFileEnv  = "STYX_BURST_PID_FILE"
)

// A Blob request whose payload begins with delayedPrefix is PARKED until the
// file named by blobReleaseEnv appears, for at most blobHoldEnv, and the handler
// appends what actually happened to the file named by blobProgressEnv. Only a
// marked payload is parked, so ordinary traffic keeps flowing at full speed
// alongside the one call a test is holding.
//
// The hold is a release signal rather than a fixed delay because a fixed delay
// only makes the call PROBABLY still unanswered when the test gets where it is
// going. Held on a signal, the call is unanswered by construction — and the
// blobHoldExpired line is what keeps a hold that ran out on its own from being
// mistaken for one the test released.
//
// The progress file is this fixture's general channel for reporting what it is
// doing while it is still doing it: every state only this process can observe is
// appended there, and a test waits on the line it needs.
const (
	blobHoldEnv     = "STYX_BURST_BLOB_HOLD"
	blobReleaseEnv  = "STYX_BURST_BLOB_RELEASE"
	blobProgressEnv = "STYX_BURST_BLOB_PROGRESS"
	delayedPrefix   = "delay:"
)

// The three lines a held Blob handler reports, and how often it looks for its
// release.
const (
	blobParked      = "burstplugin: blob handler parked"
	blobReleased    = "burstplugin: blob handler released"
	blobHoldExpired = "burstplugin: blob hold expired"
	blobPollEvery   = 2 * time.Millisecond
)

// serveExitGraceful and serveExitFailed are the two lines a test greps for.
const (
	serveExitGraceful = "burstplugin: serve exited gracefully"
	serveExitFailed   = "burstplugin: serve failed"
)

type echoServer struct{}

// A Say request carrying slowMessage is PARKED until the file named by
// slowReleaseEnv appears, for at most slowHoldEnv, so a test can tear the
// instance down while its serving loop is inside dispatch rather than parked in a
// receive — the two are different arrival orders for the same teardown.
//
// It is a release signal rather than a delay because what the ordering has to be
// is causal. Entry is a state of THIS process's serve loop, so the handler
// reports that it parked and a test waits for that report; and the handler then
// stays parked until that test, having watched the teardown actually start,
// releases it. A fixed delay makes both ends of that ordering merely probable —
// dispatch may not have been reached yet when the teardown begins, and the
// handler may have returned before it does. The expiry line is what keeps a hold
// that ran out on its own from passing as one a test released.
const (
	slowMessage    = "sleep"
	slowHoldEnv    = "STYX_BURST_SLOW_HOLD"
	slowReleaseEnv = "STYX_BURST_SLOW_RELEASE"
)

// The three lines a held Say handler reports.
const (
	slowParked      = "burstplugin: slow handler parked"
	slowReleased    = "burstplugin: slow handler released"
	slowHoldExpired = "burstplugin: slow hold expired"
)

// crashMessage kills the process from inside a handler, so a test can drive the
// crash-restart path with the burst path active.
const crashMessage = "crash"

// burstDownMessage takes this instance's burst socket down and leaves everything
// else alone, so a test can drive the loss of the burst path ALONE across a real
// process boundary — the one fault a host cannot inject from its own side, since
// the descriptor belongs to the plugin.
const burstDownMessage = "burstdown"

// The two lines the burst-socket takedown reports, appended to the progress file.
// The failure line carries the candidate count, so a test asserting the takedown
// happened can tell "the socket could not be identified" from "the socket was
// identified and shutting it down failed".
const (
	burstDownDone   = "burstplugin: burst socket taken down"
	burstDownFailed = "burstplugin: burst socket not taken down"
)

func (echoServer) Say(_ context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	switch req.GetMessage() {
	case slowMessage:
		holdSlow()
	case crashMessage:
		os.Exit(2)
	case burstDownMessage:
		takeBurstSocketDown()
	}

	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

// takeBurstSocketDown ends both directions of this process's burst socket,
// leaving every other descriptor it holds open.
//
// The socket is identified by its kind rather than by its number: a descriptor
// received over SCM_RIGHTS lands on whatever number was free, and the plugin
// server never hands it to a plugin. A styx plugin process holds exactly one
// AF_UNIX SOCK_STREAM descriptor — the control plane is SOCK_SEQPACKET, the
// region is a memfd, the wakeups are eventfds, and stdio is pipes — so the
// identification is unambiguous, and the candidate count is reported so a test
// can assert that rather than assume it.
//
// It shuts the socket down rather than closing it. Closing a descriptor another
// goroutine is parked reading on does not reliably wake that goroutine, and frees
// the number for something else to be handed; a shutdown ends both directions of
// a descriptor that stays open, which is exactly what the peer sees when the
// connection dies.
func takeBurstSocketDown() {
	progress := os.Getenv(blobProgressEnv)

	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		appendLine(progress, fmt.Sprintf("%s: %v", burstDownFailed, err))

		return
	}

	var candidates []int
	for _, entry := range entries {
		fd, cerr := strconv.Atoi(entry.Name())
		if cerr != nil {
			continue
		}
		domain, derr := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_DOMAIN)
		if derr != nil {
			continue // not a socket
		}
		sockType, terr := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
		if terr == nil && domain == unix.AF_UNIX && sockType == unix.SOCK_STREAM {
			candidates = append(candidates, fd)
		}
	}
	if len(candidates) != 1 {
		appendLine(progress, fmt.Sprintf("%s: %d candidate descriptors", burstDownFailed, len(candidates)))

		return
	}
	if serr := unix.Shutdown(candidates[0], unix.SHUT_RDWR); serr != nil {
		appendLine(progress, fmt.Sprintf("%s: %v", burstDownFailed, serr))

		return
	}
	appendLine(progress, burstDownDone)
}

func (echoServer) Blob(_ context.Context, req *echopb.BlobRequest) (*echopb.BlobResponse, error) {
	holdBlob(req.GetPayload())

	return &echopb.BlobResponse{Payload: req.GetPayload()}, nil
}

// holdBlob parks a marked request until the test releases it.
func holdBlob(payload []byte) {
	if !bytes.HasPrefix(payload, []byte(delayedPrefix)) {
		return
	}

	parkUntilReleased(blobHoldEnv, blobReleaseEnv, blobParked, blobReleased, blobHoldExpired)
}

// holdSlow parks a slowMessage request until the test releases it.
func holdSlow() {
	parkUntilReleased(slowHoldEnv, slowReleaseEnv, slowParked, slowReleased, slowHoldExpired)
}

// parkUntilReleased parks this handler until the file named by releaseEnv
// appears, for at most the duration named by holdEnv, announcing that it has
// parked first — so a test that sees that announcement knows the call is accepted
// and its answer does not exist — and reporting on the way out whether the
// release arrived or the hold ran out first.
//
// A hold nobody configured parks nothing: the handler answers immediately, which
// is what every test that is not holding this call wants.
func parkUntilReleased(holdEnv, releaseEnv, parked, released, expired string) {
	raw := os.Getenv(holdEnv)
	if raw == "" {
		return
	}
	hold, err := time.ParseDuration(raw)
	if err != nil {
		return
	}
	release := os.Getenv(releaseEnv)
	if release == "" {
		return
	}

	progress := os.Getenv(blobProgressEnv)
	appendLine(progress, parked)

	deadline := time.Now().Add(hold)
	for time.Now().Before(deadline) {
		if _, serr := os.Stat(release); serr == nil {
			appendLine(progress, released)

			return
		}
		time.Sleep(blobPollEvery)
	}
	appendLine(progress, expired)
}

// report appends one line to the exit file, if the test asked for one.
func report(line string) { appendLine(os.Getenv(exitFileEnv), line) }

// appendLine adds one line to path, if a test named one. Appending rather than
// writing keeps every line a fixture produces, whichever instance produced it.
func appendLine(path, line string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = fmt.Fprintln(f, line)
}

func main() {
	appendLine(os.Getenv(pidFileEnv), strconv.Itoa(os.Getpid()))

	// Advertising only uds is how a test drives the host's transport fallback with
	// a burst ceiling configured: the negotiated tuple is uds, so the burst path is
	// dormant however the ceiling is set.
	cfg := styx.PluginServerConfig{}
	if os.Getenv(udsOnlyEnv) != "" {
		cfg.Transports = []styx.Transport{styx.TransportUDS}
	}

	srv := styx.NewPluginServer(cfg)
	echopb.RegisterEchoServer(srv, echoServer{})

	if err := srv.Serve(); err != nil {
		report(fmt.Sprintf("%s: %v", serveExitFailed, err))
		os.Exit(1)
	}
	report(serveExitGraceful)
}
