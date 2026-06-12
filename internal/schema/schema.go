package schema

import (
	"context"
	"database/sql"
	"embed"
	_ "embed"
	"fmt"
	"io/fs"

	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

//go:embed ddl
var ddlFS embed.FS

// Create applies the PostgreSQL schema to the given database.
func Create(ctx context.Context, db *sql.DB) error {
	return xsql.Transact(
		ctx,
		db,
		func(ctx context.Context, tx *sql.Tx) error {
			// Lock the schema to prevent concurrent creation attempts from interfering
			// with each other. This is necessary because CREATE SCHEMA IF NOT EXISTS
			// suffers a long-standing race condition.
			//
			// https://stackoverflow.com/questions/29900845/create-schema-if-not-exists-raises-duplicate-key-error
			if _, err := tx.ExecContext(
				ctx,
				`LOCK TABLE pg_catalog.pg_namespace IN SHARE ROW EXCLUSIVE MODE`,
			); err != nil {
				return fmt.Errorf("unable to lock schema: %w", err)
			}

			if err := fs.WalkDir(
				ddlFS,
				".",
				func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						return fmt.Errorf("unable to read embedded schema: %w", err)
					}

					if d.IsDir() {
						return nil
					}

					ddl, err := ddlFS.ReadFile(path)
					if err != nil {
						return fmt.Errorf("unable to read embedded schema: %w", err)
					}

					if _, err := tx.ExecContext(ctx, string(ddl)); err != nil {
						return fmt.Errorf("unable to execute DDL: %w", err)
					}

					return nil
				},
			); err != nil {
				return err
			}

			return nil
		},
	)
}

// Drop removes the engine's schema from the given database.
func Drop(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS dogma CASCADE`); err != nil {
		return fmt.Errorf("unable to drop schema: %w", err)
	}

	return nil
}
