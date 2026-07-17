# Styx

Styx is a Go plugin framework for local, same-machine, process-isolated plugin communication. It replaces gRPC-over-Unix-domain-socket with a shared-memory data plane (descriptor rings, payload arena, eventfd wakeups), targeting unary RPC round-trips in the low single-digit microseconds. Plugins are separate executables with their own runtime and crash boundary; users define services in standard protobuf `service` blocks and get gRPC-style generated clients and servers, seeing no shared-memory details.

## Installation

```bash
go get github.com/arloliu/styx
```

## Quickstart

Define a service in protobuf:

```protobuf
syntax = "proto3";
package echo;

service Echo {
  rpc Say(SayRequest) returns (SayResponse);
}

message SayRequest { string message = 1; }
message SayResponse { string message = 1; }
```

Generate client and server stubs:

```bash
protoc --go_out=. --go-styx_out=. echo.proto
```

Implement and serve the plugin (`examples/echo/plugin/main.go`):

```go
package main

import (
	"context"
	"os"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

type echoServer struct{}

func (echoServer) Say(ctx context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

func main() {
	srv := styx.NewPluginServer()
	echopb.RegisterEchoServer(srv, echoServer{})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
```

Call it from the host (`examples/echo/host/main.go`):

```go
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: echo-host <plugin-path>")
		os.Exit(2)
	}

	host := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:     "echo",
			Path:     os.Args[1],
			Services: []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})

	// Supervisor events are a subscription, never a callback invoked on an
	// internal goroutine holding a lock — a real host observes them like
	// this instead of polling.
	go func() {
		for ev := range host.Events() {
			fmt.Fprintf(os.Stderr, "event: plugin=%s kind=%d err=%v\n", ev.Plugin, ev.Kind, ev.Err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := host.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer func() { _ = host.Stop(ctx) }()

	client := echopb.NewEchoClient(host.Plugin("echo"))

	resp, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "say:", err)
		os.Exit(1)
	}

	fmt.Println(resp.GetMessage())
}
```

## Status

**Complete:** Full framework on UDS transport. Handshake with version negotiation, supervised lifecycle management with restart policy, crash detection, comprehensive error taxonomy.

**Earlier spike:** Validated the shared-memory performance premise. See [docs/plans/2026-07-16-m0-gate-report.md](docs/plans/2026-07-16-m0-gate-report.md).

**Next:** SHM transport implementation (memfd rings + arena + eventfd wakeups).

**Planned:** Streaming RPC.

For the full design, see [docs/specs/2026-07-16-styx-design.md](docs/specs/2026-07-16-styx-design.md).
