// Command externalgeometry compiles a program that uses only public APIs to
// configure shared-memory geometry. Successful compilation proves the geometry
// API is self-contained and does not leak internal types to external users.
package main

import "github.com/arloliu/styx"

func main() {
	// Both profile helpers return a fully-configured public geometry.
	_ = styx.GeometryDefault()
	_ = styx.GeometryLean()

	// An explicit geometry built entirely from public types.
	explicit := styx.ShmGeometry{
		RingCapacity:     1024,
		LifecycleReserve: 64,
		HostToPlugin: []styx.ShmSizeClass{
			{SlabSize: 64, SlabCount: 512},
			{SlabSize: 4096, SlabCount: 128},
		},
		PluginToHost: []styx.ShmSizeClass{
			{SlabSize: 64, SlabCount: 512},
			{SlabSize: 4096, SlabCount: 128},
		},
	}

	// Every host-side shared-memory knob on PluginSpec is set from public types.
	cfg := styx.HostConfig{
		Plugins: []styx.PluginSpec{
			{Name: "explicit", Path: "/nonexistent", Transport: "shm", Geometry: explicit, MaxDataInflight: 128, StrictCapacity: true},
			{Name: "default-profile", Path: "/nonexistent", Transport: "auto", Geometry: styx.GeometryDefault()},
			{Name: "lean-profile", Path: "/nonexistent", Transport: "shm", Geometry: styx.GeometryLean(), MaxDataInflight: 32, StrictCapacity: true},
			{Name: "uds", Path: "/nonexistent", Transport: "uds"},
		},
	}
	_ = styx.NewHost(cfg)
}
