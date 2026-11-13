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

// moduleVersion is a module's path, and its version if it is known.
type moduleVersion struct {
	Path    string
	Version string
}

// buildInfo is the running binary's build information.
var buildInfo, hasBuildInfo = debug.ReadBuildInfo()

// versions is the versions of the modules in the running binary.
var versions = readModuleVersions()

// moduleOf returns the module that contains the package with the given import
// path, which is the module among the binary's own module and its dependencies
// with the longest path that the package's path begins with. It returns false
// if the binary's build information has no such module.
func moduleOf(packagePath string) (moduleVersion, bool) {
	if !hasBuildInfo || packagePath == "" {
		return moduleVersion{}, false
	}

	var best moduleVersion

	consider := func(path, version string) {
		contains := packagePath == path || strings.HasPrefix(packagePath, path+"/")
		if contains && len(path) > len(best.Path) {
			best = moduleVersion{path, version}
		}
	}

	consider(buildInfo.Main.Path, builtFromVersion())

	for _, dep := range buildInfo.Deps {
		if dep.Replace != nil {
			consider(dep.Path, formatModuleVersion(dep.Replace.Version))
		} else {
			consider(dep.Path, formatModuleVersion(dep.Version))
		}
	}

	return best, best.Path != ""
}

// builtFromVersion returns the version of the module that the binary is built
// from. It is the version that Go stamps on the module, or else the first seven
// characters of the hash of the commit that the binary was built from.
func builtFromVersion() string {
	if v := formatModuleVersion(buildInfo.Main.Version); v != "" {
		return v
	}

	for _, s := range buildInfo.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 7 {
			return s.Value[:7]
		}
	}

	return ""
}

// readModuleVersions reads the versions from the binary's build information.
//
// A module that the binary depends on has the version that the binary's go.mod
// requires, unless that module is replaced by a directory, which has no
// version.
//
// A binary that is built from a module inside the runkit repository, such as
// one of the inspector's demos, was built from a checkout of runkit, so its
// commit is runkit's commit, even if the binary replaces its dependency on
// runkit with a directory.
func readModuleVersions() moduleVersions {
	if !hasBuildInfo {
		return moduleVersions{}
	}

	dependency := func(path string) string {
		for _, dep := range buildInfo.Deps {
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

	versions := moduleVersions{
		Dogma: dependency(dogmaModule),
	}

	if buildInfo.Main.Path == runkitModule || strings.HasPrefix(buildInfo.Main.Path, runkitModule+"/") {
		versions.Runkit = builtFromVersion()
	} else {
		versions.Runkit = dependency(runkitModule)
	}

	return versions
}

// pseudoVersion matches a pseudo-version, which names a commit that has no
// tag, and captures the first 12 characters of the commit's hash.
var pseudoVersion = regexp.MustCompile(`^v\d+\.\d+\.\d+-(?:[0-9A-Za-z.-]+\.)?\d{14}-([0-9a-f]{12})`)

// formatModuleVersion returns the text to show for a module version. A release
// is shown as its tag, and a pseudo-version as the first seven characters of
// its commit's hash. It returns an empty string if the version is empty or means
// "built from a directory".
func formatModuleVersion(v string) string {
	switch v {
	case "", "(devel)":
		return ""
	}

	if m := pseudoVersion.FindStringSubmatch(v); m != nil {
		return m[1][:7]
	}

	return v
}
