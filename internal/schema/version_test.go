package schema_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	. "github.com/dogmatiq/runkit/internal/schema"
	"github.com/dogmatiq/runkit/internal/schema/internal/migrations"
	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

func mustParseVersion(t testing.TB, text string) version.Version {
	t.Helper()

	v, err := version.Parse(text)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

// changelogSchemaVersion matches the line that starts the first paragraph of
// each section of the changelog, such as "Schema version: v2.0.0 (new)".
var changelogSchemaVersion = regexp.MustCompile(`^Schema version: (v\S+) \((new|existing)\)$`)

// matchChangelogSchemaVersion matches the first line after the heading of the
// given changelog section against changelogSchemaVersion. It returns nil if
// the line doesn't match.
func matchChangelogSchemaVersion(section string) []string {
	_, body, _ := strings.Cut(section, "\n")
	line, _, _ := strings.Cut(strings.TrimSpace(body), "\n")
	return changelogSchemaVersion.FindStringSubmatch(line)
}

// changelogRelease is the schema version that the changelog states for a
// released runkit version.
type changelogRelease struct {
	// RunkitVersion is the tag of the runkit version, such as "v0.2.0".
	RunkitVersion string

	// SchemaVersion is the schema version that the runkit version creates.
	SchemaVersion version.Version

	// IsNew is true if the runkit version is the first with that schema
	// version.
	IsNew bool
}

// changelogHeading matches the heading of a section of the changelog, after the
// "## [" that starts it, and captures the runkit version and the date. A
// section whose date is the placeholder "YYYY-MM-DD" is not released.
var changelogHeading = regexp.MustCompile(`^(\d+\.\d+\.\d+)\] - (\S+)`)

// readChangelogReleases returns the release of each released section of the
// changelog, newest first.
//
// It fails the test if a released section doesn't start with a schema version
// line.
func readChangelogReleases(t testing.TB) []changelogRelease {
	t.Helper()

	content, err := os.ReadFile("../../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}

	var releases []changelogRelease

	// The first element is the preamble, which precedes the first section.
	for _, section := range strings.Split(string(content), "\n## [")[1:] {
		heading := changelogHeading.FindStringSubmatch(section)
		if heading == nil || heading[2] == "YYYY-MM-DD" {
			continue // the unreleased section, or one that was never released
		}

		line := matchChangelogSchemaVersion(section)
		if line == nil {
			t.Fatalf("the changelog section of runkit %s doesn't start with a line like \"Schema version: v2.0.0 (new)\"", heading[1])
		}

		releases = append(releases, changelogRelease{
			RunkitVersion: "v" + heading[1],
			SchemaVersion: mustParseVersion(t, line[1]),
			IsNew:         line[2] == "new",
		})
	}

	return releases
}

// schemaVersionOf returns the schema version that the changelog states for the
// given released runkit version, such as "v0.2.0".
func schemaVersionOf(t testing.TB, runkitVersion string) version.Version {
	t.Helper()

	for _, r := range readChangelogReleases(t) {
		if r.RunkitVersion == runkitVersion {
			return r.SchemaVersion
		}
	}

	t.Fatalf("the changelog has no released section for runkit %s", runkitVersion)
	return version.Version{}
}

func TestChangelogSchemaVersion(t *testing.T) {
	content, err := os.ReadFile("../../CHANGELOG.md")
	if err != nil {
		t.Fatal(err)
	}

	// The first element is the preamble, which precedes the first section.
	sections := strings.Split(string(content), "\n## [")[1:]
	if len(sections) < 2 {
		t.Fatal("expected the changelog to have at least two sections")
	}

	topmost := matchChangelogSchemaVersion(sections[0])
	if topmost == nil {
		t.Fatal("the first paragraph of the topmost changelog section must be a line like \"Schema version: v2.0.0 (new)\"")
	}

	if got, want := mustParseVersion(t, topmost[1]), TargetVersion; got != want {
		t.Fatalf("the topmost changelog section states schema version %s, but the current schema version is %s", got, want)
	}

	below := matchChangelogSchemaVersion(sections[1])
	if below == nil {
		t.Fatal("the first paragraph of the changelog section below the topmost one must be a line like \"Schema version: v1.1.0 (existing)\"")
	}

	previous := mustParseVersion(t, below[1])

	wantMarker := "existing"
	if previous != TargetVersion {
		wantMarker = "new"
	}

	if topmost[2] != wantMarker {
		t.Fatalf(
			"the topmost changelog section says (%s), but the section below it is at schema version %s, so it should say (%s)",
			topmost[2],
			previous,
			wantMarker,
		)
	}
}

func TestTargetVersion(t *testing.T) {
	t.Run("it is not below the schema version of any migration", func(t *testing.T) {
		for _, m := range migrations.All() {
			if TargetVersion.Compare(m.Version) < 0 {
				t.Fatalf(
					"the migration to %s is above the target schema version, %s",
					m.Version,
					TargetVersion,
				)
			}
		}
	})
}
