// Module externalgeometry is a compile-only fixture that imports the public
// styx module from OUTSIDE it, to prove every public shared-memory geometry form
// (ShmGeometry, the profile helpers, and the PluginSpec knobs) is importable and
// configurable without reaching any internal package. It is built, never run, by
// the root package's external-geometry compile test.
module externalgeometry

go 1.26.0

require github.com/arloliu/styx v0.0.0

require (
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/arloliu/styx => ../..
