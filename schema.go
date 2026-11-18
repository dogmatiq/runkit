package runkit

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/runkit/internal/schema"
)

// ApplySchemaOnline brings the database's schema to the schema version of
// this runkit version, creating the schema if the database doesn't have one
// yet. It applies only the changes that are safe to apply while engines are
// running.
//
// If the schema change needs every engine to be stopped, it returns an error
// that says so, and changes nothing. Use [ApplySchemaOffline] in that case.
//
// Creating the schema needs the CREATE privilege on the database. Changing an
// existing schema needs the role that owns its objects.
//
// It is safe to call from several engines at once. A call that has changes to
// make waits for the others to finish first.
func ApplySchemaOnline(ctx context.Context, db *sql.DB) error {
	return schema.Apply(
		ctx,
		db,
		false,
		schemaLockTimeout,
		schemaStatementTimeout,
	)
}

// ApplySchemaOffline does what [ApplySchemaOnline] does and also applies a
// schema change that raises the major number of the schema version. Stop every
// engine that uses the database before calling it; the database itself must be
// running.
//
// Nothing checks that no engine is running. It needs the same privileges as
// [ApplySchemaOnline].
func ApplySchemaOffline(ctx context.Context, db *sql.DB) error {
	return schema.Apply(
		ctx,
		db,
		true,
		schemaLockTimeout,
		schemaStatementTimeout,
	)
}

// DropSchema drops the engine's PostgreSQL schema, named "runkit", and
// everything in it.
func DropSchema(ctx context.Context, db *sql.DB) error {
	return schema.Drop(ctx, db)
}
