// Package projection runs the dispatch loop for a single
// [dogma.ProjectionMessageHandler].
//
// A projection handler builds a read-model from the application's event
// stream. The handler owns its checkpoint — it is responsible for persisting
// and reporting how far it has progressed — while this package is responsible
// for delivering events to the handler in order and invoking
// [dogma.ProjectionMessageHandler.Compact] periodically.
package projection
