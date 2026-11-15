package schema_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	. "github.com/dogmatiq/runkit/internal/schema"
)

func mustParseVersion(t testing.TB, text string) Version {
	t.Helper()

	v, err := ParseVersion(text)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

func TestParseVersion(t *testing.T) {
	t.Run("when the text is valid", func(t *testing.T) {
		cases := []struct {
			Text                string
			Major, Minor, Patch uint
		}{
			{"v0.0.0", 0, 0, 0},
			{"v2.0.0", 2, 0, 0},
			{"v1.12.3", 1, 12, 3},
			{"v10.20.30", 10, 20, 30},
		}

		for _, tc := range cases {
			t.Run(tc.Text, func(t *testing.T) {
				v := mustParseVersion(t, tc.Text)

				got := [3]uint{v.Major(), v.Minor(), v.Patch()}
				want := [3]uint{tc.Major, tc.Minor, tc.Patch}

				if got != want {
					t.Fatalf("unexpected numbers: want %v, got %v", want, got)
				}

				if got := v.String(); got != tc.Text {
					t.Fatalf("unexpected result from String(): want %q, got %q", tc.Text, got)
				}
			})
		}
	})

	t.Run("when the text is invalid", func(t *testing.T) {
		cases := []struct {
			Name string
			Text string
		}{
			{"empty", ""},
			{"no prefix", "2.0.0"},
			{"prefix only", "v"},
			{"missing patch", "v2.0"},
			{"extra number", "v2.0.0.1"},
			{"pre-release", "v2.0.0-rc.1"},
			{"build metadata", "v2.0.0+build"},
			{"leading zero", "v02.0.0"},
			{"negative", "v-1.0.0"},
			{"non-numeric", "v1.a.0"},
			{"empty number", "v1..0"},
			{"space", "v 1.0.0"},
			{"too large", "v99999999999.0.0"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				if _, err := ParseVersion(tc.Text); err == nil {
					t.Fatalf("expected an error for %q, got nil", tc.Text)
				}
			})
		}
	})
}

func TestVersion(t *testing.T) {
	t.Run("func Compare()", func(t *testing.T) {
		cases := []struct {
			Name  string
			V     string
			Other string
			Want  int
		}{
			{"lower major", "v1.9.9", "v2.0.0", -1},
			{"higher major", "v2.0.0", "v1.9.9", +1},
			{"lower minor", "v2.0.9", "v2.1.0", -1},
			{"higher minor", "v2.1.0", "v2.0.9", +1},
			{"lower patch", "v2.1.0", "v2.1.1", -1},
			{"higher patch", "v2.1.1", "v2.1.0", +1},
			{"equal", "v2.1.1", "v2.1.1", 0},
			{"numeric, not textual, order", "v2.9.0", "v2.10.0", -1},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				v := mustParseVersion(t, tc.V)
				other := mustParseVersion(t, tc.Other)

				if got := v.Compare(other); got != tc.Want {
					t.Fatalf("%s.Compare(%s): want %d, got %d", v, other, tc.Want, got)
				}
			})
		}
	})

	t.Run("func CanRunOn()", func(t *testing.T) {
		cases := []struct {
			Name     string
			Engine   string
			Database string
			Want     bool
		}{
			{"same schema version", "v2.1.0", "v2.1.0", true},
			{"database has a newer patch", "v2.1.0", "v2.1.5", true},
			{"database has an older patch", "v2.1.3", "v2.1.0", true},
			{"database has a newer minor", "v2.1.0", "v2.2.0", true},
			{"database has an older minor", "v2.1.0", "v2.0.9", false},
			{"database has an older major", "v2.1.0", "v1.9.0", false},
			{"database has a newer major", "v2.1.0", "v3.0.0", false},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				engine := mustParseVersion(t, tc.Engine)
				database := mustParseVersion(t, tc.Database)

				if got := engine.CanRunOn(database); got != tc.Want {
					t.Fatalf("%s.CanRunOn(%s): want %t, got %t", engine, database, tc.Want, got)
				}
			})
		}
	})
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

	if got, want := mustParseVersion(t, topmost[1]), CurrentVersion; got != want {
		t.Fatalf("the topmost changelog section states schema version %s, but the current schema version is %s", got, want)
	}

	// Sections written before the line existed are at the schema version of
	// the first runkit version that has the
	previous := mustParseVersion(t, "v1.0.0")
	if below := matchChangelogSchemaVersion(sections[1]); below != nil {
		previous = mustParseVersion(t, below[1])
	}

	wantMarker := "existing"
	if previous != CurrentVersion {
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
