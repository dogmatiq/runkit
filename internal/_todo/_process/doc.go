// Package process runs the dispatch loop for a single
// [dogma.ProcessMessageHandler], delivering events and deadlines to its
// instances and persisting their effects.
//
// A [Controller] polls for new work across all instances of one handler,
// while per-instance worker goroutines serialise message handling within
// each instance. Multi-replica safety relies on row-level database locks
// alone — there are no advisory locks, leases, or owner identifiers.
package process
