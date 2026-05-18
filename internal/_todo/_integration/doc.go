// Package integration runs the dispatch loop for one
// [dogma.IntegrationMessageHandler].
//
// Two concurrency modes are supported, derived from the handler's
// [dogma.ConcurrencyPreference]: singleton mode runs at most one in-flight
// command of this handler globally; max-parallel mode runs many drain workers
// gated by an engine-wide semaphore shared by every max-parallel integration
// handler in this replica.
package integration
