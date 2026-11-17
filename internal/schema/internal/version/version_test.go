package version_test

import (
	"testing"

	. "github.com/dogmatiq/runkit/internal/schema/internal/version"
)

func mustParse(t testing.TB, text string) Version {
	t.Helper()

	v, err := Parse(text)
	if err != nil {
		t.Fatal(err)
	}

	return v
}

func TestParse(t *testing.T) {
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
				v := mustParse(t, tc.Text)

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
				if _, err := Parse(tc.Text); err == nil {
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
				v := mustParse(t, tc.V)
				other := mustParse(t, tc.Other)

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
				engine := mustParse(t, tc.Engine)
				database := mustParse(t, tc.Database)

				if got := engine.CanRunOn(database); got != tc.Want {
					t.Fatalf("%s.CanRunOn(%s): want %t, got %t", engine, database, tc.Want, got)
				}
			})
		}
	})

	t.Run("func IsZero()", func(t *testing.T) {
		cases := []struct {
			Name    string
			Version string
			Want    bool
		}{
			{"v0.0.0", "v0.0.0", true},
			{"only the patch number is set", "v0.0.1", false},
			{"only the minor number is set", "v0.1.0", false},
			{"only the major number is set", "v1.0.0", false},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				v := mustParse(t, tc.Version)

				if got := v.IsZero(); got != tc.Want {
					t.Fatalf("%s.IsZero(): want %t, got %t", v, tc.Want, got)
				}
			})
		}
	})

	t.Run("func IsSatisfiedBy()", func(t *testing.T) {
		cases := []struct {
			Name     string
			Target   string
			Database string
			Want     bool
		}{
			{"same schema version", "v2.1.3", "v2.1.3", true},
			{"database has a newer patch", "v2.1.3", "v2.1.5", true},
			{"database has an older patch", "v2.1.3", "v2.1.0", false},
			{"database has a newer minor", "v2.1.3", "v2.2.0", true},
			{"database has an older minor", "v2.1.3", "v2.0.9", false},
			{"database has an older major", "v2.1.3", "v1.9.0", false},
			{"database has a newer major", "v2.1.3", "v3.0.0", false},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				target := mustParse(t, tc.Target)
				database := mustParse(t, tc.Database)

				if got := target.IsSatisfiedBy(database); got != tc.Want {
					t.Fatalf("%s.IsSatisfiedBy(%s): want %t, got %t", target, database, tc.Want, got)
				}
			})
		}
	})

	t.Run("func CanUpgradeOffline()", func(t *testing.T) {
		cases := []struct {
			Name     string
			Target   string
			Database string
			Want     bool
		}{
			{"same schema version", "v2.1.3", "v2.1.3", true},
			{"database has a newer minor", "v2.1.3", "v2.2.0", true},
			{"database has an older minor", "v2.1.3", "v2.0.9", true},
			{"database has an older major", "v2.1.3", "v1.9.0", true},
			{"database has a newer major", "v2.1.3", "v3.0.0", false},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				target := mustParse(t, tc.Target)
				database := mustParse(t, tc.Database)

				if got := target.CanUpgradeOffline(database); got != tc.Want {
					t.Fatalf("%s.CanUpgradeOffline(%s): want %t, got %t", target, database, tc.Want, got)
				}
			})
		}
	})

	t.Run("func CanUpgradeOnline()", func(t *testing.T) {
		cases := []struct {
			Name     string
			Target   string
			Database string
			Want     bool
		}{
			{"same schema version", "v2.1.3", "v2.1.3", true},
			{"database has a newer minor", "v2.1.3", "v2.2.0", true},
			{"database has an older minor", "v2.1.3", "v2.0.9", true},
			{"database has an older major", "v2.1.3", "v1.9.0", false},
			{"database has a newer major", "v2.1.3", "v3.0.0", false},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				target := mustParse(t, tc.Target)
				database := mustParse(t, tc.Database)

				if got := target.CanUpgradeOnline(database); got != tc.Want {
					t.Fatalf("%s.CanUpgradeOnline(%s): want %t, got %t", target, database, tc.Want, got)
				}
			})
		}
	})
}
