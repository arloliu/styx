---
okf_version: "0.2"
swept_at: 2dab436
---

# Units

## Public surface

* [styx](styx/) - the root package, the only public import: Host, PluginServer, ClientConn, streams, errors.
* [codec](codec/) - Codec and SizedMarshaler interfaces, with the protobuf implementation as the default.
* [observe](observe/) - metrics, logging, and tracing hook interfaces, free of any vendored stack.
* [supervisor](supervisor/) - the public RestartPolicy configuration surface.
* [cmd/protoc-gen-go-styx](cmd/protoc-gen-go-styx/) - the protoc/buf plugin generating Styx clients and servers.

## RPC runtime and transport

* [internal/rpcruntime](internal/rpcruntime/) - request and stream tables, credit flow control, deadlines, chunking, burst, dispatch.
* [internal/transport](internal/transport/) - the Send/Recv transport abstraction and the Unix-domain-socket implementation.
* [internal/transport/shm](internal/transport/shm/) - the single writer goroutine and two-lane intent queue over one ring and arena.

## Shared-memory core

* [internal/ring](internal/ring/) - the SPSC descriptor ring; part of the unsafe core.
* [internal/arena](internal/arena/) - the per-direction size-classed slab allocator; part of the unsafe core.
* [internal/shm](internal/shm/) - memfd-backed region creation, sealing, mmap lifecycle, layout decode, generation tracking.
* [internal/event](internal/event/) - the eventfd-backed spin-then-park waiter for cross-process wakeup.

## Process and control plane

* [internal/lifecycle](internal/lifecycle/) - spawn, death-signal bootstrap, reload, rollback, restore, and the teardown state machine.
* [internal/supervisor](internal/supervisor/) - spawn/heartbeat supervision, health classification, restart policy execution, crash capture.
* [internal/control](internal/control/) - the control-plane protocol: handshake, fd passing, liveness, drain, shutdown.

## Spanning several units

* [crosscutting](crosscutting/) - mechanics no single package's doc comment can state whole.

## Support

* [internal/observeq](internal/observeq/) - the observability dispatch queue keeping reporting off the hot path.
* [internal/panics](internal/panics/) - shared panic recovery and value-to-text conversion, owned by no layer.
