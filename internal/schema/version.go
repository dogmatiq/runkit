package schema

import (
	"fmt"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

// TargetVersion is the schema version that this runkit version creates and
// requires.
var TargetVersion = version.Version{2, 0, 0}

// checkVersion reports whether a database at the current schema version needs
// its schema created or upgraded to reach the target schema version.
//
// It returns an error if the target can't upgrade the database, or if it can't
// upgrade it unless offline is true. The zero [version.Version] as current stands for a
// database that has no schema.
func checkVersion(
	current, target version.Version,
	offline bool,
) (needsUpgrade bool, err error) {
	switch {
	case current.IsZero():
		return true, nil

	case !target.CanUpgradeOffline(current):
		return false, fmt.Errorf(
			"incompatible schema version: this version of dogmatiq/runkit (%s, schema %s) is too old to work with the existing database schema (%s)",
			runkitVersion(),
			target,
			current,
		)

	case !target.CanUpgradeOnline(current) && !offline:
		return false, fmt.Errorf(
			"manual schema migration required: this version of dogmatiq/runkit (%s, schema %s) requires a later major version of the schema than the existing database schema (%s)",
			runkitVersion(),
			target,
			current,
		)

	default:
		return !target.IsSatisfiedBy(current), nil
	}
}
