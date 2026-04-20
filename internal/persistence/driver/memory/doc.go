// Package memory implements the [persistence.Provider] interface using in-process
// data structures. Providers that share a silo name operate on the same
// underlying stores, allowing multiple engine nodes to share state within a
// single process during tests.
package memory
