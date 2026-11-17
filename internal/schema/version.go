package schema

import (
	"fmt"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

// TargetVersion is the schema version that this runkit version creates and
// requires.
var TargetVersion = version.Version{2, 0, 0}

// needsUpgrade reports whether a database at the current schema version needs
// its schema created or upgraded to reach the target schema version.
//
// migratingTo is the schema version of the migration in progress, if there is
// one. The database is then partly at that schema version.
//
// It returns an error if no upgrade lets the target use the database. That is
// the case when the current schema version, or the schema version of the
// migration in progress, has a higher major number than the target.
//
// The zero [version.Version] as current stands for a database that has no
// schema, and as migratingTo for a database with no migration in progress.
func needsUpgrade(
	current, migratingTo, target version.Version,
) (needed bool, err error) {
	switch {
	case current.IsZero():
		return true, nil

	case !target.CanUpgradeOffline(current):
		return false, fmt.Errorf(
			"incompatible database schema: runkit %s (schema %s) is too old for the database, which is already at %s",
			runkitVersion(),
			compatibleRange(target),
			current,
		)

	case migratingTo.Major() > target.Major():
		return false, migrationInProgressError(migratingTo, target)

	default:
		return !target.IsSatisfiedBy(current), nil
	}
}

// checkCanUpgrade returns an error if the target can't upgrade a database that
// needs an upgrade, according to [needsUpgrade], or if it can't upgrade it
// unless offline is true.
//
// The caller must hold the lock that [Apply] takes. A migration in progress is
// then one that an earlier call left unfinished. Without the lock, it could be
// one that another call is about to finish.
func checkCanUpgrade(
	current, migratingTo, target version.Version,
	offline bool,
) error {
	switch {
	case migratingTo.Compare(target) > 0:
		// The target has no migration to a schema version above itself, so it
		// can't finish this one.
		return migrationInProgressError(migratingTo, target)

	case !current.IsZero() && !target.CanUpgradeOnline(current) && !offline:
		return fmt.Errorf(
			"major database schema upgrade required: runkit %s requires schema %s, but the database is at %s: stop every instance of the application, then apply the upgrade offline",
			runkitVersion(),
			compatibleRange(target),
			current,
		)

	default:
		return nil
	}
}

// migrationInProgressError returns the error for a database that the target
// can't use or upgrade because of the migration in progress.
func migrationInProgressError(migratingTo, target version.Version) error {
	return fmt.Errorf(
		"incompatible database schema: runkit %s (schema %s) is too old for the database, which has an in-progress migration to %s",
		runkitVersion(),
		compatibleRange(target),
		migratingTo,
	)
}

// compatibleRange returns the schema versions that satisfy the target, written
// as a caret range such as "^v2.0.0". A caret range matches the given version
// and any later version that has the same major number.
func compatibleRange(target version.Version) string {
	return "^" + target.String()
}
