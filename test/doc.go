// Package test will hold the multi-node integration and chaos test harness:
// spinning up in-process clusters, crashing nodes (Phase 4), and simulating
// network partitions between arbitrary node pairs without killing processes
// (Phase 6).
//
// Unit tests live next to the code they test; this package is only for
// whole-cluster scenarios.
package test
