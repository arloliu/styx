module github.com/arloliu/styx-bench-goplugin

go 1.26.0

require (
	github.com/arloliu/go-plugin v1.9.0
	github.com/arloliu/styx v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)

replace github.com/arloliu/styx => ../..
