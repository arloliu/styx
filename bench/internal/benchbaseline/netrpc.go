package benchbaseline

import (
	"net"
	"net/rpc"
	"os"
)

// EchoArgs holds a payload for the Echo RPC call.
type EchoArgs struct{ Payload []byte }

// EchoReply holds the response payload from the Echo RPC call.
type EchoReply struct{ Payload []byte }

type echoService struct{}

func (echoService) Echo(args *EchoArgs, reply *EchoReply) error {
	reply.Payload = append([]byte(nil), args.Payload...)
	return nil
}

// NetRPCBaseline uses the stdlib net/rpc package (gob codec) over a Unix domain socket.
type NetRPCBaseline struct {
	sockPath string
	ln       net.Listener
	client   *rpc.Client
}

// NewNetRPC returns a new NetRPCBaseline.
func NewNetRPC() *NetRPCBaseline { return &NetRPCBaseline{} }

func (n *NetRPCBaseline) Name() string { return "net-rpc-uds" }

func (n *NetRPCBaseline) Start() error {
	f, err := os.CreateTemp("", "styx-spike-netrpc-*.sock")
	if err != nil {
		return err
	}
	path := f.Name()
	_ = f.Close()
	_ = os.Remove(path)
	n.sockPath = path

	srv := rpc.NewServer()
	if err := srv.RegisterName("Echo", echoService{}); err != nil {
		return err
	}
	// Start has no ctx param, matching every other baseline in this package.
	//nolint:noctx
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	n.ln = ln
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()

	client, err := rpc.Dial("unix", path)
	if err != nil {
		return err
	}
	n.client = client

	return nil
}

func (n *NetRPCBaseline) Call(payload []byte) ([]byte, error) {
	var reply EchoReply
	if err := n.client.Call("Echo.Echo", &EchoArgs{Payload: payload}, &reply); err != nil {
		return nil, err
	}

	return reply.Payload, nil
}

func (n *NetRPCBaseline) Stop() error {
	if n.client != nil {
		_ = n.client.Close()
	}
	if n.ln != nil {
		_ = n.ln.Close()
	}
	// Listener.Close already unlinks the socket file.
	// os.IsNotExist here means we lost the race to unlink, not a teardown failure.
	if err := os.Remove(n.sockPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
