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
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
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

More runnable examples are under [`examples/`](examples/): host-side streaming
of all three shapes ([`examples/streaming/`](examples/streaming/)) and a
state-preserving hot-reload ([`examples/hot-reload/`](examples/hot-reload/)).
Coming from `hashicorp/go-plugin`? See
[docs/migration-from-go-plugin.md](docs/migration-from-go-plugin.md).

## Status

The framework is complete on both data-plane transports:

- **Transports.** Shared memory (memfd rings + payload arena + eventfd wakeups)
  and Unix domain sockets. Each plugin selects its transport on `PluginSpec`:
  the default offers shared memory, preferred, and falls back to UDS only when a
  plugin does not offer shared memory — never a silent downgrade when shared
  memory is pinned. At a 64-byte payload with one in-flight call, the measured
  round-trip p50 is 2.11 µs on shared memory versus 8.15 µs on UDS
  ([bench/shm/REPORT.md](bench/shm/REPORT.md)).
- **RPC.** Unary and streaming (server-, client-, and bidirectional streaming),
  with version-negotiated handshake, deadline and cancellation propagation, and
  a comprehensive, retryability-classified error taxonomy. Under load, a
  shared-memory send whose ring or arena is full applies typed backpressure —
  deadline-bounded for streaming sends, while a starved unary send can outlive
  its call deadline (see the migration guide's provisioning section);
  provisioning the geometry for peak concurrency (or opting into
  `StrictCapacity`) avoids it.
- **Lifecycle.** Supervised plugins with a restart policy, crash detection, a
  progress-based heartbeat, and hot-reload that hands a plugin's sealed,
  verified state to a freshly spawned successor without dropping accepted calls
  or restarting supervision.

The shared-memory data plane is validated by a differential test suite against
the UDS oracle, a fault-injection (chaos) suite, and a long-running leak soak.
Its full performance profile is in [bench/shm/REPORT.md](bench/shm/REPORT.md);
the earlier prototype that first validated the premise is in
[docs/plans/2026-07-16-m0-gate-report.md](docs/plans/2026-07-16-m0-gate-report.md).

For the full design, see [docs/specs/2026-07-16-styx-design.md](docs/specs/2026-07-16-styx-design.md).
