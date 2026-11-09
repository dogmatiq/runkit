package cluster

import (
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/zeebo/xxh3"
)

// FindResponsibleNode returns the ID of the node that is responsible for the
// work with the given rendezvous key. It returns false if there are no live
// nodes.
//
// If the key is an exact match for the ID of a live node, that node is
// responsible. Otherwise, it uses [rendezvous hashing] to choose one of the
// live nodes, so it returns the same ID wherever it's called, given the same
// set of live nodes.
//
// [rendezvous hashing]: https://en.wikipedia.org/wiki/Rendezvous_hashing
func FindResponsibleNode(key *uuidpb.UUID, liveNodes *uuidcollection.Set) (*uuidpb.UUID, bool) {
	if liveNodes.Has(key) {
		return key, true
	}

	var responsible scoredNode

	for nodeID := range liveNodes.All() {
		node := scoreNode(key, nodeID)

		if node.Outranks(responsible) {
			responsible = node
		}
	}

	return responsible.ID, responsible.ID != nil
}

// isResponsibleFor returns true if, among the given live nodes, the node with
// the given ID is the one responsible for the work with the given rendezvous
// key.
//
// It gives the same answer as comparing the result of [FindResponsibleNode]
// with the node's ID, but stops as soon as another node outranks it.
func isResponsibleFor(
	nodeID *uuidpb.UUID,
	key *uuidpb.UUID,
	liveNodes *uuidcollection.Set,
) bool {
	// A node that isn't live is responsible for nothing.
	if !liveNodes.Has(nodeID) {
		return false
	}

	// A live node is always responsible for the work with its own ID as the
	// key.
	if key.Equal(nodeID) {
		return true
	}

	// If the key is the ID of another live node, that node is responsible.
	if liveNodes.Has(key) {
		return false
	}

	// Otherwise, the node is responsible unless another node outranks it.
	self := scoreNode(key, nodeID)

	for peerID := range liveNodes.All() {
		if peerID.Equal(nodeID) {
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
	ID *uuidpb.UUID

	// Score is the node's score for the rendezvous key.
	Score uint64
}

// scoreNode returns the node with the given ID, scored for the given
// rendezvous key.
func scoreNode(key, nodeID *uuidpb.UUID) scoredNode {
	var buf [32]byte
	uuidpb.CopyBytes(key, buf[:16])
	uuidpb.CopyBytes(nodeID, buf[16:])

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
	if other.ID == nil {
		return true
	}

	if n.Score != other.Score {
		return n.Score > other.Score
	}

	return n.ID.Less(other.ID)
}
