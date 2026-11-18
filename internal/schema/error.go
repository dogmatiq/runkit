package schema

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

// UpgradeFailedError is returned when [Apply] decides that the database's
// schema needs an upgrade, and the upgrade doesn't finish.
//
// The database's recorded schema version is still Current, and the database
// may be part-way through a migration from it. Required.CanRunOn(Current)
// reports whether this runkit version can run on the schema as it is.
type UpgradeFailedError struct {
	// Current is the database's schema version, which the upgrade didn't
	// change. It is the zero version if the database had no schema.
	Current version.Version

	// Required is the schema version that this runkit version needs.
	Required version.Version

	// Cause is the error that stopped the upgrade.
	Cause error
}

// Error returns the message of the error that stopped the upgrade.
func (e UpgradeFailedError) Error() string {
	return e.Cause.Error()
}

// Unwrap returns the error that stopped the upgrade.
func (e UpgradeFailedError) Unwrap() error {
	return e.Cause
}

// IncompatibleSchemaError is returned when the database's schema version has a
// higher major number than the target, so this runkit version can't use the
// database.
type IncompatibleSchemaError struct {
	// Current is the database's schema version.
	Current version.Version

	// Required is the schema version that this runkit version needs.
	Required version.Version
}

func (e IncompatibleSchemaError) Error() string {
	return fmt.Sprintf(
		"incompatible database schema: runkit %s (schema ^%s) is too old for the database, which is already at %s",
		runkitVersion(),
		e.Required,
		e.Current,
	)
}

// IncompatibleMigrationError is returned when the database has a migration in
// progress to a schema version that this runkit version can't use or finish.
type IncompatibleMigrationError struct {
	// MigratingTo is the schema version of the migration in progress.
	MigratingTo version.Version

	// HighestAvailable is the highest schema version that this runkit version
	// can bring a database to.
	HighestAvailable version.Version
}

func (e IncompatibleMigrationError) Error() string {
	return fmt.Sprintf(
		"incompatible database schema: runkit %s (schema ^%s) is too old for the database, which has an in-progress migration to %s",
		runkitVersion(),
		e.HighestAvailable,
		e.MigratingTo,
	)
}

// InvalidIndexesError is returned when the database needs an upgrade and has
// invalid indexes in the engine's schema. The upgrade can't go ahead until a
// person drops or rebuilds each of them.
type InvalidIndexesError struct {
	// Indexes are the names of the invalid indexes, each qualified by its
	// PostgreSQL schema, in ascending order.
	Indexes []string
}

func (e InvalidIndexesError) Error() string {
	return fmt.Sprintf(
		"database schema upgrade requires human intervention: invalid indexes need to be dropped or rebuilt manually (%s)",
		strings.Join(e.Indexes, ", "),
	)
}

// OfflineUpgradeRequiredError is returned when the database can only be
// upgraded offline, and the upgrade isn't offline. That is the case when the
// upgrade raises the major number of the database's schema version.
type OfflineUpgradeRequiredError struct {
	// Current is the database's schema version.
	Current version.Version

	// Required is the schema version that this runkit version needs.
	Required version.Version
}

func (e OfflineUpgradeRequiredError) Error() string {
	return fmt.Sprintf(
		"major database schema upgrade required: runkit %s requires schema ^%s, but the database is at %s: stop every instance of the application, then apply the upgrade offline",
		runkitVersion(),
		e.Required,
		e.Current,
	)
}

// runkitVersion returns the version of the runkit module that is compiled into
// the program, or "unknown version" if the build doesn't record it, such as in
// runkit's own tests.
func runkitVersion() string {
	const unknown = "unknown version"

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return unknown
	}

	modules := append([]*debug.Module{&info.Main}, info.Deps...)

	for _, m := range modules {
		if m.Path != "github.com/dogmatiq/runkit" {
			continue
		}

		if m.Replace != nil {
			m = m.Replace
		}

		if m.Version != "" && m.Version != "(devel)" {
			return m.Version
		}
	}

	return unknown
}
