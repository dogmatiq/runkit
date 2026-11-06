package cluster

import (
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/zeebo/xxh3"
)

// Membership is the set of live nodes in the cluster, as the local [Node] sees
// it at one moment.
//
// A nil Membership has no live nodes, so the local node is responsible for no
// workloads.
type Membership struct {
	nodeID *uuidpb.UUID
	isLive bool
	peers  *uuidcollection.Set
}

// Peers returns the IDs of the live nodes other than the local node.
//
// The returned set must not be modified.
func (m *Membership) Peers() *uuidcollection.Set {
	if m == nil {
		return nil
	}

	return m.peers
}

// LocalNodeIsResponsibleFor returns true if the local node is responsible for
// the given rendezvous workload.
//
// Each live node is scored by hashing the workload with the node's ID, and the
// node with the highest score is responsible. Ties are broken by the lower ID,
// so every node with the same membership reaches the same answer, regardless
// of the order in which it holds the live nodes.
//
// The local node is responsible for nothing while it can not hear its own
// heartbeats.
func (m *Membership) LocalNodeIsResponsibleFor(workload *uuidpb.UUID) bool {
	if m == nil || !m.isLive {
		return false
	}

	// The first half of buf holds the workload, and the second half holds the
	// ID of the node being scored.
	var buf [32]byte
	uuidpb.CopyBytes(workload, buf[:16])

	uuidpb.CopyBytes(m.nodeID, buf[16:])
	localScore := xxh3.Hash(buf[:])

	for peerID := range m.peers.All() {
		uuidpb.CopyBytes(peerID, buf[16:])
		peerScore := xxh3.Hash(buf[:])

		if peerScore > localScore {
			return false
		}

		if peerScore == localScore && peerID.Less(m.nodeID) {
			return false
		}
	}

	return true
}
