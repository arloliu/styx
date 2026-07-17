package main

import "google.golang.org/protobuf/reflect/protoreflect"

// ExportServiceID re-exports serviceID for generate_test (external test
// package `main_test`, matching the export_test.go convention used at the
// module root for Host/ClientConn's own unexported internals).
func ExportServiceID(fullName protoreflect.FullName) uint64 {
	return serviceID(fullName)
}

// ExportMethodID re-exports methodID for generate_test.
func ExportMethodID(service protoreflect.FullName, method string) uint64 {
	return methodID(service, method)
}

// ExportCheckCollisions re-exports checkCollisions for generate_test.
func ExportCheckCollisions(ids map[uint64]protoreflect.FullName, id uint64, name protoreflect.FullName) error {
	return checkCollisions(ids, id, name)
}
