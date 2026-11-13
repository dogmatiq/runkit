package pages

import (
	"regexp"
	"runtime/debug"
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
func readModuleVersions() moduleVersions {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return moduleVersions{}
	}

	find := func(path string) string {
		if info.Main.Path == path {
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

	return moduleVersions{
		Runkit: find(runkitModule),
		Dogma:  find(dogmaModule),
	}
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
