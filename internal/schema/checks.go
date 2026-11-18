package schema

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

// needsUpgrade reports whether a database at the current schema version needs
// its schema created or upgraded to reach the target schema version.
//
// migratingTo is the schema version of the migration in progress, if there is
// one. The database is then partly at that schema version.
//
// It returns an error if this runkit version can't use the database, even
// after an upgrade. That is the case when the current schema version, or the
// schema version of the migration in progress, has a higher major number than
// the target.
//
// The zero [version.Version] as current stands for a database that has no
// schema, and as migratingTo for a database with no migration in progress.
func needsUpgrade(
	current, migratingTo, target version.Version,
) (needed bool, err error) {
	switch {
	case current.IsZero():
		return true, nil

	case !canUpgradeOffline(current, target):
		return false, IncompatibleSchemaError{
			Current:  current,
			Required: target,
		}

	case !canUpgradeOffline(migratingTo, target):
		return false, IncompatibleMigrationError{
			MigratingTo:      migratingTo,
			HighestAvailable: target,
		}

	default:
		return !target.IsSatisfiedBy(current), nil
	}
}

// checkCanFinishMigration returns an error if the database has a migration in
// progress to a schema version above highestAvailable, the highest schema
// version that this runkit version can bring a database to. This runkit version
// can't finish such a migration.
//
// The caller must hold the lock that [Apply] takes. A migration in progress is
// then one that an earlier call left unfinished. Without the lock, it could be
// one that another call is about to finish.
func checkCanFinishMigration(migratingTo, highestAvailable version.Version) error {
	if migratingTo.Compare(highestAvailable) > 0 {
		return IncompatibleMigrationError{
			MigratingTo:      migratingTo,
			HighestAvailable: highestAvailable,
		}
	}

	return nil
}

// checkIndexesAreValid returns an error that names each invalid index in the
// engine's schemas, if there are any.
func checkIndexesAreValid(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(
		ctx,
		`SELECT format('%I.%I', n.nspname, c.relname)
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE NOT i.indisvalid
		AND n.nspname = ANY($1)
		ORDER BY 1`,
		schemaNames(),
	)
	if err != nil {
		return fmt.Errorf("unable to look for invalid indexes: %w", err)
	}
	defer rows.Close()

	var names []string

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("unable to look for invalid indexes: %w", err)
		}

		names = append(names, name)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to look for invalid indexes: %w", err)
	}

	if len(names) == 0 {
		return nil
	}

	return InvalidIndexesError{
		Indexes: names,
	}
}

// checkCanUpgradeMajorVersion returns an error if upgrading a database at the
// current schema version to the target raises the major number, which is only
// allowed when offline is true.
//
// The zero [version.Version] as current stands for a database that has no
// schema, which is created without raising anything.
func checkCanUpgradeMajorVersion(
	current, target version.Version,
	offline bool,
) error {
	if current.IsZero() || offline || canUpgradeOnline(current, target) {
		return nil
	}

	return OfflineUpgradeRequiredError{
		Current:  current,
		Required: target,
	}
}

// canUpgradeOffline reports whether a runkit version that needs the target
// schema version can upgrade a database at the given schema version while no
// engine is using the database. It can unless the database's schema version has
// a higher major number.
func canUpgradeOffline(database, target version.Version) bool {
	return target.Major() >= database.Major()
}

// canUpgradeOnline reports whether a runkit version that needs the target
// schema version can upgrade a database at the given schema version while
// engines are using the database. It can if the two schema versions have the
// same major number.
func canUpgradeOnline(database, target version.Version) bool {
	return target.Major() == database.Major()
}
