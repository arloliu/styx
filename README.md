# Styx

[![Go Reference](https://pkg.go.dev/badge/github.com/arloliu/styx.svg)](https://pkg.go.dev/github.com/arloliu/styx)
[![Release](https://img.shields.io/github/v/release/arloliu/styx)](https://github.com/arloliu/styx/releases)

Styx is a Go plugin framework for local, same-machine, process-isolated plugin communication. It replaces gRPC-over-Unix-domain-socket with a shared-memory data plane (descriptor rings, payload arena, eventfd wakeups), targeting unary RPC round-trips in the low single-digit microseconds. Plugins are separate executables with their own runtime and crash boundary; users define services in standard protobuf `service` blocks and get gRPC-style generated clients and servers, seeing no shared-memory details.

## Why "Styx"?

In the old maps of the underworld, the Styx is the river between two worlds —
the boundary itself. That's a plugin framework: your host lives in one
process, your plugin in another world entirely, with its own runtime and its
own crash domain. When it dies, it dies over there. The boundary is the
point — but a boundary you can't cross efficiently is just a wall, so
everything depends on the ferry.

Styx is the ferry, and the fare is nearly nothing. Both banks touch the same
water: a sealed shared-memory region — descriptor rings, a slab arena,
eventfd wakeups — carries a unary round trip in ~2.4 µs where gRPC-over-UDS
takes ~16. One fixed-size memfd, resident memory pay-as-you-touch. No
daemons, no sidecars — one river, two banks.

The mythology holds up under load. The gods swore unbreakable oaths on the
Styx — ours are the frozen [`shm-abi.md`](docs/specs/shm-abi.md) and
[`stream-protocol.md`](docs/specs/stream-protocol.md). Achilles was dipped in
it and came out nearly invulnerable — this transport was dipped in a chaos
suite, a differential oracle, and a leak soak. And with state-preserving hot
reload, Styx is the rare river you can cross back over: a plugin goes down,
its state ferries home, its successor picks up where it left off.

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
  rpc Blob(BlobRequest) returns (BlobResponse);
}

message SayRequest { string message = 1; }
message SayResponse { string message = 1; }

message BlobRequest { bytes payload = 1; }
message BlobResponse { bytes payload = 1; }
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

// Blob echoes the payload back unchanged. Unlike Say's string field, bytes
// avoids the extra []byte<->string conversion on both sides of the call, so
// this is the representative shape for bulk binary payloads.
func (echoServer) Blob(ctx context.Context, req *echopb.BlobRequest) (*echopb.BlobResponse, error) {
	return &echopb.BlobResponse{Payload: req.GetPayload()}, nil
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
of all three shapes ([`examples/streaming/`](examples/streaming/)), a
state-preserving hot-reload ([`examples/hot-reload/`](examples/hot-reload/)),
a handler slow enough to backpressure its caller, with the counters that name it
([`examples/slow-handler/`](examples/slow-handler/)), and a real consumer's
device-plugin lifecycle contract defined as an ordinary Styx service
([`examples/device-gateway/`](examples/device-gateway/); see
[docs/device-gateway-integration.md](docs/device-gateway-integration.md)
for the full contract).
Coming from `hashicorp/go-plugin`? See
[docs/migration-from-go-plugin.md](docs/migration-from-go-plugin.md).

For what graceful shutdown, crash/restart, and hot-reload actually do to a running
plugin — and what it means when a reload fails — see [docs/plugin-lifecycle.md](docs/plugin-lifecycle.md).

## Configuration

The Quickstart above leaves every `PluginSpec` and `PluginServerConfig` field at
its default. Real deployments usually set at least one of: `Transport` (which
data-plane transport to negotiate — shared memory, Unix domain sockets, or let
the host pick), `Geometry` (the shape and capacity of the shared-memory
region), `Restart` (the crash-restart policy), or `Services` (the version
range a host requires from a plugin).

See [docs/configuration.md](docs/configuration.md) for the full field-by-field
guide, including a plain-language walkthrough of shared-memory geometry (ring
capacity, lifecycle reserve, size classes) for readers who don't already know
those terms.

## Observability

`host.Events()` (shown in the Quickstart above) is a subscription to every
plugin's lifecycle transitions — spawned, ready, unhealthy, crashed,
restarting, or given up for good. `host.Stop` closes it once it has torn the
host down — after a brief wait for you to take what the shutdown published —
so the `range` loop above ends with the host rather than outliving it. Give
`Stop` a context with its own budget (not an already-canceled one, such as the
context you started under): a `Stop` that returns before tearing anything down
closes nothing and leaves that loop waiting forever, and a `Host` whose
teardown has completed is done — `Start` on it fails with `ErrHostStopped`,
so build a new one to reconnect.
`HostConfig.Logger` and `HostConfig.Metrics`
cover structured diagnostics and counters for the same transitions, so a real
host usually configures all three rather than reimplementing logging inside
an `Events()` consumer.
See [docs/supervisor-events.md](docs/supervisor-events.md) for what each event
means and what's worth reacting to versus just logging.

## Status

Styx is feature-complete on both data-plane transports — shared memory and
Unix domain sockets, unary and streaming RPC, supervised plugin lifecycle
with hot-reload — and validated by a differential test suite against the
UDS oracle, a fault-injection (chaos) suite, and a long-running leak soak.

Current release: **v0.1.0** (see [CHANGELOG.md](CHANGELOG.md)). Pre-1.0: the
public Go API may still move between minor versions; the wire contracts
([`shm-abi.md`](docs/specs/shm-abi.md),
[`stream-protocol.md`](docs/specs/stream-protocol.md)) are frozen and change
only by explicit, versioned amendment.

At a 64-byte payload with one in-flight call, a unary round trip measures a p50
of 2.4 µs over shared memory, against 7.7 µs over Unix domain sockets and
15.9 µs over gRPC-over-UDS. Against `hashicorp/go-plugin`, Styx over shared
memory is faster in all 24 cells of the comparison matrix — **1.65× to 4.72×**
on throughput, across payloads from 64 B to 1 MiB and concurrency from 1 to 64.

See [docs/benchmark.md](docs/benchmark.md) for what each suite measures, how to
reproduce every number, and which of them CI actually gates;
[docs/performance-headroom.md](docs/performance-headroom.md) records why the
transport is not faster still and which optimization levers remain open.

For the full design, see [docs/specs/2026-07-16-styx-design.md](docs/specs/2026-07-16-styx-design.md).
