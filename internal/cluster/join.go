package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"uuid"

	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Join returns the ID of the cluster that uses the database, and creates the
// cluster if the database has none.
//
// A cluster is the set of nodes that share one database, and it runs a single
// application. The first call for a database creates the cluster, with a random
// ID. Every later call, from any node, returns that same ID.
//
// If the cluster's application has the same key as app, it updates the stored
// name of the application to the name in app. It returns an error if the
// cluster's application has a different key.
func Join(
	ctx context.Context,
	db *sql.DB,
	app *identitypb.Identity,
) (uuid.UUID, error) {
	var (
		id      uuid.UUID
		appKey  uuid.UUID
		appName string
	)

	// The conflicting row is always updated, even if only to its existing
	// values, so that RETURNING yields the database's cluster. The new ID in
	// VALUES is used only when the database has no cluster.
	if err := db.QueryRowContext(
		ctx,
		`INSERT INTO runkit.cluster AS c (
			id,
			application_key,
			application_name
		) VALUES (
			$1,
			$2,
			$3
		)
		ON CONFLICT ((TRUE)) DO UPDATE
		SET application_name = CASE
			WHEN c.application_key = excluded.application_key
			THEN excluded.application_name
			ELSE c.application_name
		END
		RETURNING id, application_key, application_name`,
		xsql.UUID(uuid.New()),
		xsql.UUID(app.GetKey().AsStd()),
		app.GetName(),
	).Scan(
		xsql.UUID(&id),
		xsql.UUID(&appKey),
		&appName,
	); err != nil {
		return uuid.UUID{}, err
	}

	if appKey != app.GetKey().AsStd() {
		return uuid.UUID{}, fmt.Errorf(
			"it already runs application %s",
			identitypb.New(appName, uuidpb.FromStd(appKey)),
		)
	}

	return id, nil
}
