package application

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Bind binds the database to the application with the given identity, if it is
// not already bound to an application.
//
// If the database is bound to an application with the same key, the
// application's name is updated to match id. It returns an error if the
// database is bound to an application with a different key.
func Bind(ctx context.Context, db *sql.DB, id *identitypb.Identity) error {
	var (
		key  uuidpb.UUID
		name string
	)

	// The conflicting row is always updated, even if only to its existing
	// values, so that RETURNING yields the identity the database is bound to.
	if err := db.QueryRowContext(
		ctx,
		`INSERT INTO application.identity AS i (
			key,
			name
		) VALUES (
			$1,
			$2
		)
		ON CONFLICT (singleton) DO UPDATE
		SET name = CASE
			WHEN i.key = excluded.key THEN excluded.name
			ELSE i.name
		END
		RETURNING key, name`,
		xsql.UUID(id.GetKey()),
		id.GetName(),
	).Scan(
		xsql.UUID(&key),
		&name,
	); err != nil {
		return err
	}

	if key.Equal(id.GetKey()) {
		return nil
	}

	return fmt.Errorf(
		"it is already bound to application %s",
		identitypb.New(name, &key),
	)
}
