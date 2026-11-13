package pages

import (
	"regexp"
	"runtime/debug"
	"strings"
)

const (
	runkitModule = "github.com/dogmatiq/runkit"
	dogmaModule  = "github.com/dogmatiq/dogma"
)

// moduleVersions is the versions of the modules that the footer names. A
// version is empty if the binary doesn't say which version it was built with.
type moduleVersions struct {
	Runkit string
	Dogma  string
}

// versions is the versions of the modules in the running binary.
var versions = readModuleVersions()

// readModuleVersions reads the versions from the binary's build information.
//
// A module that the binary depends on has the version that the binary's go.mod
// requires, unless that module is replaced by a directory, which has no
// version. The module that the binary is built from has the version that Go
// stamps on it, or the commit that the binary was built from.
//
// A binary that is built from a module inside the runkit repository, such as
// one of the inspector's demos, was built from a checkout of runkit, so its
// commit is runkit's commit, even if the binary replaces its dependency on
// runkit with a directory.
func readModuleVersions() moduleVersions {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return moduleVersions{}
	}

	dependency := func(path string) string {
		for _, dep := range info.Deps {
			if dep.Path != path {
				continue
			}

			if dep.Replace != nil {
				return formatModuleVersion(dep.Replace.Version)
			}

			return formatModuleVersion(dep.Version)
		}

		return ""
	}

	// builtFrom returns the version of the module that the binary is built
	// from.
	builtFrom := func() string {
		if v := formatModuleVersion(info.Main.Version); v != "" {
			return v
		}

		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 7 {
				return "commit " + s.Value[:7]
			}
		}

		return ""
	}

	versions := moduleVersions{
		Dogma: dependency(dogmaModule),
	}

	if info.Main.Path == runkitModule || strings.HasPrefix(info.Main.Path, runkitModule+"/") {
		versions.Runkit = builtFrom()
	} else {
		versions.Runkit = dependency(runkitModule)
	}

	return versions
}

// pseudoVersion matches a pseudo-version, which names a commit that has no
// tag, and captures the first 12 characters of the commit's hash.
var pseudoVersion = regexp.MustCompile(`^v\d+\.\d+\.\d+-(?:[0-9A-Za-z.-]+\.)?\d{14}-([0-9a-f]{12})`)

// formatModuleVersion returns the text to show for a module version. It returns
// an empty string if the version is empty or means "built from a directory".
func formatModuleVersion(v string) string {
	switch v {
	case "", "(devel)":
		return ""
	}

	if m := pseudoVersion.FindStringSubmatch(v); m != nil {
		return "commit " + m[1][:7]
	}

	return v
}
