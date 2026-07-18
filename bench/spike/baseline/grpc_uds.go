package baseline

import (
	"context"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/arloliu/styx/bench/spike/baseline/pingpb"
)

type pingServer struct {
	pingpb.UnimplementedPingServer
}

func (pingServer) Echo(_ context.Context, req *pingpb.EchoRequest) (*pingpb.EchoResponse, error) {
	return &pingpb.EchoResponse{Payload: req.Payload}, nil
}

// GRPCUDSBaseline runs the Ping gRPC service over a Unix domain socket.
type GRPCUDSBaseline struct {
	sockPath string
	srv      *grpc.Server
	conn     *grpc.ClientConn
	client   pingpb.PingClient
}

func NewGRPCUDS() *GRPCUDSBaseline { return &GRPCUDSBaseline{} }

func (g *GRPCUDSBaseline) Name() string { return "grpc-uds" }

func (g *GRPCUDSBaseline) Start() error {
	f, err := os.CreateTemp("", "styx-spike-grpc-*.sock")
	if err != nil {
		return err
	}
	path := f.Name()
	_ = f.Close()
	_ = os.Remove(path)
	g.sockPath = path

	//nolint:noctx // benchmark-harness Start has no ctx param; matches every other baseline in this package
	ln, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	g.srv = grpc.NewServer()
	pingpb.RegisterPingServer(g.srv, pingServer{})
	go func() { _ = g.srv.Serve(ln) }()

	conn, err := grpc.NewClient("unix:"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	g.conn = conn
	g.client = pingpb.NewPingClient(conn)

	return nil
}

func (g *GRPCUDSBaseline) Call(payload []byte) ([]byte, error) {
	resp, err := g.client.Echo(context.Background(), &pingpb.EchoRequest{Payload: payload})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

func (g *GRPCUDSBaseline) Stop() error {
	if g.conn != nil {
		_ = g.conn.Close()
	}
	if g.srv != nil {
		g.srv.GracefulStop()
	}
	// GracefulStop closes the listener, which already unlinks the socket
	// file it created; os.IsNotExist here just means we lost that race,
	// not a teardown failure.
	if err := os.Remove(g.sockPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
