// Package schematest provides helpers for tests that need a database with the
// schema that a released runkit version created.
package schematest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"testing"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// CreateSchemaOf creates the schema of the given released runkit version in the
// given database, in the way that it created the schema of a new database.
//
// schemaVersion is the schema version that the runkit version creates. Runkit
// v0.3.0 and later record it in the database. Earlier runkit versions don't
// record one, and ignore it.
//
// It fails the test if it can't fetch that runkit version.
func CreateSchemaOf(
	t testing.TB,
	db *sql.DB,
	runkitVersion string,
	schemaVersion version.Version,
) {
	t.Helper()

	out, err := exec.CommandContext(
		t.Context(),
		"go", "mod", "download", "-json",
		"github.com/dogmatiq/runkit@"+runkitVersion,
	).Output()
	if err != nil {
		t.Fatalf("unable to fetch runkit %s: %s", runkitVersion, err)
	}

	var module struct {
		Dir string
	}

	if err := json.Unmarshal(out, &module); err != nil {
		t.Fatal(err)
	}

	files := os.DirFS(module.Dir)

	// Runkit v0.2.0 and earlier keep their SQL in "internal/schema/ddl". Later
	// runkit versions keep the SQL for the table of the schema version in
	// "internal/schema/internal/bootstrap", and the current schema in
	// "internal/schema/internal/current". A runkit version with another layout
	// fails here, and this function needs to learn it.
	var created error

	if _, err := fs.Stat(files, "internal/schema/ddl"); err == nil {
		created = createSchemaFromDDL(t.Context(), db, files)
	} else {
		created = createSchemaFromCurrent(t.Context(), db, files, schemaVersion)
	}

	if created != nil {
		t.Fatalf("unable to create the schema of runkit %s: %s", runkitVersion, created)
	}
}

// createSchemaFromDDL creates the schema in the way of runkit v0.2.0 and
// earlier, which apply every file in "internal/schema/ddl" in one transaction.
func createSchemaFromDDL(ctx context.Context, db *sql.DB, files fs.FS) error {
	return xsql.Transact(
		ctx,
		db,
		func(ctx context.Context, tx *sql.Tx) error {
			// These runkit versions lock the schema before applying the DDL.
			if _, err := tx.ExecContext(
				ctx,
				`LOCK TABLE pg_catalog.pg_namespace IN SHARE ROW EXCLUSIVE MODE`,
			); err != nil {
				return err
			}

			return fs.WalkDir(
				files,
				"internal/schema/ddl",
				func(path string, d fs.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return err
					}

					return execFile(ctx, tx, files, path)
				},
			)
		},
	)
}

// createSchemaFromCurrent creates the schema in the way of runkit v0.3.0 and
// later on a new database. It applies the SQL that creates the table of the
// schema version, then the current schema, then records the given schema
// version.
func createSchemaFromCurrent(
	ctx context.Context,
	db *sql.DB,
	files fs.FS,
	schemaVersion version.Version,
) error {
	current, err := fs.Glob(files, "internal/schema/internal/current/*.sql")
	if err != nil {
		return err
	}

	paths := append(
		[]string{"internal/schema/internal/bootstrap/bootstrap.sql"},
		current...,
	)

	return xsql.Transact(
		ctx,
		db,
		func(ctx context.Context, tx *sql.Tx) error {
			for _, path := range paths {
				if err := execFile(ctx, tx, files, path); err != nil {
					return err
				}
			}

			_, err := tx.ExecContext(
				ctx,
				`INSERT INTO runkit.schema_version (version)
				VALUES ($1)`,
				schemaVersion,
			)
			return err
		},
	)
}

// execFile executes the SQL in the given file.
func execFile(ctx context.Context, tx *sql.Tx, files fs.FS, path string) error {
	ddl, err := fs.ReadFile(files, path)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, string(ddl)); err != nil {
		return fmt.Errorf("unable to execute %s: %w", path, err)
	}

	return nil
}
