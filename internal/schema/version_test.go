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

	// Sections written before the line existed are at the schema version of
	// the first runkit version that has the
	previous := mustParseVersion(t, "v1.0.0")
	if below := matchChangelogSchemaVersion(sections[1]); below != nil {
		previous = mustParseVersion(t, below[1])
	}

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
