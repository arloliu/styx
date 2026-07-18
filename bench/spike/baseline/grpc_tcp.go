package baseline

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/arloliu/styx/bench/spike/baseline/pingpb"
)

// GRPCTCPBaseline runs the Ping gRPC service over TCP loopback (127.0.0.1).
type GRPCTCPBaseline struct {
	srv    *grpc.Server
	ln     net.Listener
	conn   *grpc.ClientConn
	client pingpb.PingClient
}

func NewGRPCTCP() *GRPCTCPBaseline { return &GRPCTCPBaseline{} }

func (g *GRPCTCPBaseline) Name() string { return "grpc-tcp-loopback" }

func (g *GRPCTCPBaseline) Start() error {
	//nolint:noctx // benchmark-harness Start has no ctx param; matches every other baseline in this package
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	g.ln = ln
	g.srv = grpc.NewServer()
	pingpb.RegisterPingServer(g.srv, pingServer{})
	go func() { _ = g.srv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	g.conn = conn
	g.client = pingpb.NewPingClient(conn)

	return nil
}

func (g *GRPCTCPBaseline) Call(payload []byte) ([]byte, error) {
	resp, err := g.client.Echo(context.Background(), &pingpb.EchoRequest{Payload: payload})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

func (g *GRPCTCPBaseline) Stop() error {
	if g.conn != nil {
		_ = g.conn.Close()
	}
	if g.srv != nil {
		g.srv.GracefulStop()
	}

	return nil
}
