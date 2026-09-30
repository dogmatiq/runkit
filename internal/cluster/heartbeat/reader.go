package heartbeat

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Reader reads the set of live nodes from the heartbeat table.
type Reader struct {
	// DB is the database connection used to read heartbeat records.
	DB *sql.DB
}

// LiveNodes returns the IDs of the nodes whose heartbeat records have not
// expired.
func (r *Reader) LiveNodes(ctx context.Context) ([]*uuidpb.UUID, error) {
	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT node_id
		FROM cluster.heartbeats
		WHERE expires_at > clock_timestamp()`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query live nodes: %w", err)
	}
	defer rows.Close()

	var nodes []*uuidpb.UUID

	for rows.Next() {
		id := &uuidpb.UUID{}
		if err := rows.Scan(xsql.UUID(id)); err != nil {
			return nil, fmt.Errorf("unable to scan node ID: %w", err)
		}
		nodes = append(nodes, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate live nodes: %w", err)
	}

	return nodes, nil
}
