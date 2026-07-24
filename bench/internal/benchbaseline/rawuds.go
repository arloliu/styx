package benchbaseline

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"sync"
)

// RawUDSBaseline is a length-prefixed echo server over a Unix domain socket with no
// framing library or RPC layer — the floor for any UDS-based transport.
type RawUDSBaseline struct {
	sockPath string
	ln       net.Listener
	conn     net.Conn

	// callMu serializes Call.
	// The wire protocol carries no correlation ID; each round trip consists of two separate
	// Write() calls (length prefix, then payload), and responses are matched to requests by
	// FIFO order on one stream.
	// Two goroutines calling Call concurrently without this lock can interleave writes
	// mid-frame (e.g., A's length prefix, then B's length prefix, then A's payload) and
	// permanently desync the stream; this was confirmed by a concurrent-Call stress test
	// that hung at every run with concurrency >= 64 before this fix.
	// The benchmark suite is the first consumer to exercise this baseline concurrently;
	// at concurrency=1 callers pay an uncontended lock/unlock, which is negligible
	// compared to a syscall round trip.
	callMu sync.Mutex
}

// NewRawUDS returns a new RawUDSBaseline.
func NewRawUDS() *RawUDSBaseline { return &RawUDSBaseline{} }

func (r *RawUDSBaseline) Name() string { return "raw-uds" }

func (r *RawUDSBaseline) Start() error {
	f, err := os.CreateTemp("", "styx-spike-uds-*.sock")
	if err != nil {
		return err
	}
	path := f.Name()
	_ = f.Close()
	_ = os.Remove(path)
	r.sockPath = path

	// Start has no ctx param, matching every other baseline in this package.
	//nolint:noctx
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	r.ln = ln
	go r.serve()

	// Start has no ctx param, matching every other baseline in this package.
	//nolint:noctx
	conn, err := net.Dial("unix", path)
	if err != nil {
		return err
	}
	r.conn = conn

	return nil
}

func (r *RawUDSBaseline) Call(payload []byte) ([]byte, error) {
	r.callMu.Lock()
	defer r.callMu.Unlock()

	lenBuf := make([]byte, 4)
	// Payload is the benchmark's own generated request, well under 4GiB.
	//nolint:gosec
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payload)))
	if _, err := r.conn.Write(lenBuf); err != nil {
		return nil, err
	}
	if _, err := r.conn.Write(payload); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(r.conn, lenBuf); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf)
	out := make([]byte, n)
	if _, err := io.ReadFull(r.conn, out); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *RawUDSBaseline) Stop() error {
	if r.conn != nil {
		_ = r.conn.Close()
	}
	if r.ln != nil {
		_ = r.ln.Close()
	}
	// Listener.Close already unlinks the socket file.
	// os.IsNotExist here means we lost the race to unlink, not a teardown failure.
	if err := os.Remove(r.sockPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (r *RawUDSBaseline) serve() {
	conn, err := r.ln.Accept()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	lenBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(lenBuf)
		buf := make([]byte, n)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		if _, err := conn.Write(lenBuf); err != nil {
			return
		}
		if _, err := conn.Write(buf); err != nil {
			return
		}
	}
}
