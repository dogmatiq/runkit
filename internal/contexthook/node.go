package contexthook

import "github.com/dogmatiq/runkit/internal/cluster"

// Node is a hook type that is invoked when the engine joins the cluster.
type Node struct {
	// Node represents the engine's node in the cluster.
	Node *cluster.Node
}
