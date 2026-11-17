// Package version defines the version of the database schema.
package version

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
)

// Version identifies a release of the database schema, in the form
// `vMAJOR.MINOR.PATCH`. It says which runkit versions can run on a database. It
// follows [semantic versioning], without pre-release or build metadata.
//
// When a schema version raises the major number, engines built for the earlier
// major number can't run on the database. When it raises the minor number, the
// change is additive, so those engines keep running. When it raises only the
// patch number, engines work with or without the change, such as a corrected
// function or a faster index.
//
// Schema versions are comparable with ==.
//
// [semantic versioning]: https://semver.org/spec/v2.0.0.html
type Version [3]uint

// Parse parses a schema version in the form `vMAJOR.MINOR.PATCH`.
//
// It returns an error if the text has pre-release or build metadata, or if any
// of the three numbers has leading zeros.
func Parse(text string) (Version, error) {
	numbers, ok := strings.CutPrefix(text, "v")
	if !ok {
		return Version{}, fmt.Errorf("invalid schema version %q: missing the \"v\" prefix", text)
	}

	parts := strings.Split(numbers, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid schema version %q: expected the form vMAJOR.MINOR.PATCH", text)
	}

	var ver Version
	for i, part := range parts {
		if len(part) > 1 && part[0] == '0' {
			return Version{}, fmt.Errorf("invalid schema version %q: %q has leading zeros", text, part)
		}

		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return Version{}, fmt.Errorf("invalid schema version %q: %q is not a number", text, part)
		}

		ver[i] = uint(n)
	}

	return ver, nil
}

// Major returns the major number.
func (v Version) Major() uint {
	return v[0]
}

// Minor returns the minor number.
func (v Version) Minor() uint {
	return v[1]
}

// Patch returns the patch number.
func (v Version) Patch() uint {
	return v[2]
}

// Compare returns -1, 0 or +1 as v has lower, equal or higher precedence than
// the given schema version, as semantic versioning defines precedence.
func (v Version) Compare(other Version) int {
	return cmp.Or(
		cmp.Compare(v.Major(), other.Major()),
		cmp.Compare(v.Minor(), other.Minor()),
		cmp.Compare(v.Patch(), other.Patch()),
	)
}

// CanRunOn reports whether the runkit version with this schema version can run
// on a database at the given schema version.
func (v Version) CanRunOn(db Version) bool {
	return v.Major() == db.Major() && v.Minor() <= db.Minor()
}

// IsZero reports whether v is v0.0.0, which stands for a database that has no
// schema.
func (v Version) IsZero() bool {
	return v == Version{}
}

// IsSatisfiedBy reports whether a database at the given schema version
// satisfies this one as a requirement, as in semantic versioning: it has the
// same major number and isn't behind.
func (v Version) IsSatisfiedBy(db Version) bool {
	return v.Major() == db.Major() && v.Compare(db) <= 0
}

// CanUpgradeOffline reports whether the runkit version with this schema version
// can upgrade a database at the given schema version while no engine is using
// it, which it can unless the database has a higher major number.
func (v Version) CanUpgradeOffline(db Version) bool {
	return v.Major() >= db.Major()
}

// CanUpgradeOnline reports whether the runkit version with this schema version
// can upgrade a database at the given schema version while engines are using
// the database, which it can if they have the same major number.
func (v Version) CanUpgradeOnline(db Version) bool {
	return v.Major() == db.Major()
}

// String returns the schema version in the form `vMAJOR.MINOR.PATCH`.
func (v Version) String() string {
	return fmt.Sprintf(
		"v%d.%d.%d",
		v.Major(),
		v.Minor(),
		v.Patch(),
	)
}
