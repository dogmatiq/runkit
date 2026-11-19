package cluster

import (
	"uuid"

	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/zeebo/xxh3"
)

// FindWorkloadOwner returns the ID of the node that owns the workload with the
// given key.
//
// A workload is work that only one node should do at a time, such as consuming
// an event stream or delivering the messages in a queue. Its owner depends on
// the given live nodes, so while two nodes see different live nodes, they may
// both own the same workload.
//
// If the key is an exact match for the ID of a live node, that node is the
// owner. Otherwise, it uses [rendezvous hashing] to choose one of the live
// nodes, so it returns the same ID wherever it's called, given the same set of
// live nodes. It returns false if there are no live nodes.
//
// [rendezvous hashing]: https://en.wikipedia.org/wiki/Rendezvous_hashing
func FindWorkloadOwner(key uuid.UUID, liveNodes uuidset.SetR) (uuid.UUID, bool) {
	if liveNodes.Has(key) {
		return key, true
	}

	var owner scoredNode

	for nodeID := range liveNodes.All() {
		node := scoreNode(key, nodeID)

		if node.Outranks(owner) {
			owner = node
		}
	}

	return owner.ID, owner.ID != uuid.Nil()
}

// ownsWorkload returns true if, among the given live nodes, the node with the
// given ID owns the workload with the given key.
//
// It gives the same answer as comparing the result of [FindWorkloadOwner] with
// the node's ID, but stops as soon as another node outranks it.
func ownsWorkload(
	nodeID uuid.UUID,
	key uuid.UUID,
	liveNodes uuidset.SetR,
) bool {
	// A node that isn't live doesn't own any workloads.
	if !liveNodes.Has(nodeID) {
		return false
	}

	// A live node always owns the workload whose key is its own ID.
	if key == nodeID {
		return true
	}

	// If the key is the ID of another live node, that node is the owner.
	if liveNodes.Has(key) {
		return false
	}

	// Otherwise, the node is the owner unless another node outranks it.
	self := scoreNode(key, nodeID)

	for peerID := range liveNodes.All() {
		if peerID == nodeID {
			continue
		}

		peer := scoreNode(key, peerID)

		if peer.Outranks(self) {
			return false
		}
	}

	return true
}

// scoredNode is a live node and its score for one rendezvous key.
type scoredNode struct {
	// ID is the node's ID.
	ID uuid.UUID

	// Score is the node's score for the rendezvous key.
	Score uint64
}

// scoreNode returns the node with the given ID, scored for the given
// rendezvous key.
func scoreNode(key, nodeID uuid.UUID) scoredNode {
	var buf [32]byte
	copy(buf[:16], key[:])
	copy(buf[16:], nodeID[:])

	return scoredNode{
		ID:    nodeID,
		Score: xxh3.Hash(buf[:]),
	}
}

// Outranks returns true if it takes precedence over the given node. Every node
// outranks the zero value, which represents no node.
//
// Ties go to the lower ID, so the result doesn't depend on the order in which
// the nodes are scored.
func (n scoredNode) Outranks(other scoredNode) bool {
	if other.ID == uuid.Nil() {
		return true
	}

	if n.Score != other.Score {
		return n.Score > other.Score
	}

	return n.ID.Compare(other.ID) < 0
}
