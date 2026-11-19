package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/dogmatiq/runkit/internal/schema/internal/bootstrap"
	"github.com/dogmatiq/runkit/internal/schema/internal/current"
	"github.com/dogmatiq/runkit/internal/schema/internal/migrations"
	"github.com/dogmatiq/runkit/internal/schema/internal/upgradelock"
	"github.com/dogmatiq/runkit/internal/schema/internal/version"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Apply brings the schema of the database to [TargetVersion], creating the
// schema if the database doesn't have one yet.
//
// It does nothing if the database is at a schema version that [TargetVersion]
// can run on and isn't behind it. Only one call changes a database at a time.
// A call that has changes to make waits for the others to finish first.
//
// offline is true if the caller guarantees that no engine is using the
// database. It allows Apply to raise the major number of the schema version,
// and removes the timeouts. If the database's schema version has a lower major
// number than [TargetVersion], Apply returns an error unless offline is
// true.
//
// lockTimeout is the longest that a step waits for a lock, and stmtTimeout is
// the longest that a statement of a transaction runs. Both apply only when
// offline is false.
//
// Once Apply has decided that the database needs an upgrade, it returns any
// error wrapped in an [UpgradeFailedError], which holds the database's schema
// version.
func Apply(
	ctx context.Context,
	db *sql.DB,
	offline bool,
	lockTimeout, stmtTimeout time.Duration,
) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("unable to get a database connection: %w", err)
	}

	// An upgrade changes the session of this connection, such as by holding
	// the advisory lock and by removing the timeouts for a statement step. It
	// undoes each change, but undoing one can fail. Closing the connection,
	// rather than returning it to the pool, keeps those changes away from the
	// engine's other queries.
	defer xsql.Discard(conn)

	// Read the schema version before waiting for the lock, so that a call that
	// has nothing to change doesn't wait for another call's migration to
	// finish.
	//
	// Reading without the lock is safe, because the database never records a
	// schema version that it hasn't fully reached. A call records the target
	// version in its last transaction. A call records a version before then
	// only for a database with no version recorded. It guesses v1.0.0 or
	// v1.1.0, and the database is already at the guessed version.
	currentVersion, migratingTo, err := identifySchemaVersions(ctx, conn)
	if err != nil {
		return err
	}

	needed, err := needsUpgrade(currentVersion, migratingTo, TargetVersion)
	if err != nil || !needed {
		return err
	}

	release, err := upgradelock.Acquire(ctx, conn)
	if err != nil {
		return err
	}
	defer release()

	// Another call may have changed the schema while this one waited for the
	// lock, so read the schema version again.
	currentVersion, migratingTo, err = identifySchemaVersions(ctx, conn)
	if err != nil {
		return err
	}

	needed, err = needsUpgrade(currentVersion, migratingTo, TargetVersion)
	if err != nil || !needed {
		return err
	}

	if err := upgrade(
		ctx,
		conn,
		currentVersion,
		migratingTo,
		offline,
		lockTimeout,
		stmtTimeout,
	); err != nil {
		return UpgradeFailedError{
			Current:  currentVersion,
			Required: TargetVersion,
			Cause:    err,
		}
	}

	return nil
}

// upgrade brings the schema of a database at the current schema version to
// [TargetVersion], once [Apply] holds the lock and knows that the database
// needs an upgrade. migratingTo is the schema version of the migration in
// progress, if there is one.
func upgrade(
	ctx context.Context,
	conn *sql.Conn,
	currentVersion, migratingTo version.Version,
	offline bool,
	lockTimeout, stmtTimeout time.Duration,
) error {
	// Decide whether this call can upgrade the database only now that it
	// holds the lock. Before then, another call may have been part-way through
	// an upgrade that leaves this call with nothing to do.
	if err := checkCanFinishMigration(migratingTo, TargetVersion); err != nil {
		return err
	}

	// An interrupted CREATE INDEX CONCURRENTLY leaves its index invalid, and
	// queries ignore an invalid index. A step that builds the index again with
	// IF NOT EXISTS would find the invalid index and do nothing. The upgrade
	// can't tell whether a step builds a given index again, so it stops until a
	// person drops or rebuilds each invalid index.
	if err := checkIndexesAreValid(ctx, conn); err != nil {
		return err
	}

	// This check comes last because it is the only one that costs downtime to
	// get past: its error says to stop every instance of the application. The
	// checks above find problems that can be fixed while the application runs,
	// so they are reported before anyone stops it.
	if err := checkCanUpgradeMajorVersion(currentVersion, TargetVersion, offline); err != nil {
		return err
	}

	runner := &migrations.Runner{
		Conn:             conn,
		Offline:          offline,
		LockTimeout:      lockTimeout,
		StatementTimeout: stmtTimeout,
	}

	if err := runner.Transact(
		ctx,
		func(ctx context.Context, tx xsql.Executor) error {
			_, err := tx.ExecContext(ctx, bootstrap.SQL)
			return err
		},
	); err != nil {
		return fmt.Errorf("unable to create the table of the schema version: %w", err)
	}

	if !currentVersion.IsZero() {
		if _, err := conn.ExecContext(
			ctx,
			`INSERT INTO runkit.schema_version (version)
			VALUES ($1)
			ON CONFLICT ((TRUE)) DO NOTHING`,
			currentVersion,
		); err != nil {
			return fmt.Errorf("unable to record the schema version: %w", err)
		}

		for _, m := range migrations.All() {
			if currentVersion.Compare(m.Version) < 0 {
				runner.Migrations = append(runner.Migrations, m)
			}
		}

		if err := runner.Run(ctx); err != nil {
			return err
		}
	}

	return runner.Transact(
		ctx,
		func(ctx context.Context, tx xsql.Executor) error {
			if err := executeCurrent(ctx, tx); err != nil {
				return err
			}

			// Set the schema version only after every step and every file in
			// "current" has been applied.
			return xsql.ExecOne(
				ctx,
				tx,
				`INSERT INTO runkit.schema_version (version)
				VALUES ($1)
				ON CONFLICT ((TRUE)) DO UPDATE
				SET version = EXCLUDED.version,
					applied_at = clock_timestamp(),
					migrating_to = NULL,
					migration_steps_completed = NULL`,
				TargetVersion,
			)
		},
	)
}

// Drop drops the engine's PostgreSQL schema, named "runkit", and everything
// in it.
func Drop(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `DROP SCHEMA IF EXISTS runkit CASCADE`); err != nil {
		return fmt.Errorf("unable to drop schema: %w", err)
	}

	return nil
}

// identifySchemaVersions returns the schema version that the database has
// reached, and the schema version of the migration in progress, if there is
// one.
//
// It returns the zero [version.Version] for the database's schema version if
// the database has none of the engine's tables, and for the migration's schema
// version if no migration is in progress.
//
// Older runkit versions did not record the schema version in the database, so
// it uses the presence of certain tables to make a best-effort guess.
func identifySchemaVersions(
	ctx context.Context,
	conn *sql.Conn,
) (current, migratingTo version.Version, err error) {
	var hasMigrations, hasStreams, hasAppIdentity bool

	if err := conn.QueryRowContext(
		ctx,
		`SELECT
			to_regclass('runkit.schema_version') IS NOT NULL,
			to_regclass('eventstream.streams') IS NOT NULL,
			to_regclass('application.identity') IS NOT NULL`,
	).Scan(
		&hasMigrations,
		&hasStreams,
		&hasAppIdentity,
	); err != nil {
		return version.Version{}, version.Version{}, fmt.Errorf("unable to look for the engine's tables: %w", err)
	}

	if hasMigrations {
		var (
			v version.Version
			m sql.Null[version.Version]
		)

		err := conn.QueryRowContext(
			ctx,
			`SELECT version, migrating_to
			FROM runkit.schema_version`,
		).Scan(&v, &m)

		if err == nil {
			if v.IsZero() {
				return version.Version{}, version.Version{}, errors.New("the database has an unusable schema version: the zero version is reserved for a database that has no schema")
			}

			return v, m.V, nil
		}

		if !errors.Is(err, sql.ErrNoRows) {
			return version.Version{}, version.Version{}, fmt.Errorf("unable to read the schema version: %w", err)
		}
	}

	// The database has no record of its schema version, so it has none of a
	// migration in progress either. Make a best effort guess based on which
	// tables exist.
	switch {
	case hasAppIdentity:
		// runkit v0.2.0 added the application identity table, a new feature,
		// making it schema v1.1.0.
		return version.Version{1, 1, 0}, version.Version{}, nil
	case hasStreams:
		// The streams table has existed since runkit v0.1.x; this making it
		// schema v1.0.0.
		return version.Version{1, 0, 0}, version.Version{}, nil
	}

	return version.Version{}, version.Version{}, nil
}

// executeFile executes the SQL in the given file of [current.FS].
func executeFile(ctx context.Context, tx xsql.Executor, path string) error {
	ddl, err := current.FS.ReadFile(path)
	if err != nil {
		return fmt.Errorf("unable to read embedded schema: %w", err)
	}

	if _, err := tx.ExecContext(ctx, string(ddl)); err != nil {
		return fmt.Errorf("unable to execute %s: %w", path, err)
	}

	return nil
}

// executeCurrent executes the SQL in each file of [current.FS], in lexical
// order of the file names.
func executeCurrent(ctx context.Context, tx xsql.Executor) error {
	files, err := fs.ReadDir(current.FS, ".")
	if err != nil {
		return fmt.Errorf("unable to read embedded schema: %w", err)
	}

	// ReadDir returns the files sorted by name.
	for _, f := range files {
		if err := executeFile(ctx, tx, f.Name()); err != nil {
			return err
		}
	}

	return nil
}
