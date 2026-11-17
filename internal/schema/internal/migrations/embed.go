// Package migrations holds the migrations that upgrade an existing database,
// one directory for each schema version that a migration produces, reads them,
// and applies them, recording its progress so that a failed run resumes where
// it stopped.
package migrations

import (
	"embed"
	"fmt"
	"io/fs"
	"maps"
	"regexp"
	"slices"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

// files holds every migration directory and its step files. The pattern leaves
// out this package's Go files.
//
//go:embed */*
var files embed.FS

// All returns the embedded migrations, in ascending order of the schema version
// that each produces.
//
// It panics if a directory's name isn't a valid schema version, or if a
// directory holds anything other than step files that follow the rules of
// [parseSteps].
func All() []Migration {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		panic(err)
	}

	var all []Migration

	for _, entry := range entries {
		v, err := version.Parse(entry.Name())
		if err != nil {
			panic(fmt.Sprintf("%s: %s", entry.Name(), err))
		}

		stepFiles, err := fs.ReadDir(files, entry.Name())
		if err != nil {
			panic(err)
		}

		contents := map[string]string{}

		for _, file := range stepFiles {
			if file.IsDir() {
				panic(fmt.Sprintf("%s/%s is a directory", entry.Name(), file.Name()))
			}

			sql, err := files.ReadFile(entry.Name() + "/" + file.Name())
			if err != nil {
				panic(err)
			}

			contents[file.Name()] = string(sql)
		}

		steps, err := parseSteps(contents)
		if err != nil {
			panic(fmt.Sprintf("%s: %s", entry.Name(), err))
		}

		all = append(all, Migration{
			Version: v,
			Steps:   steps,
		})
	}

	// The directory names sort as text, which puts v10.0.0 before v2.0.0.
	slices.SortFunc(
		all,
		func(a, b Migration) int { return a.Version.Compare(b.Version) },
	)

	return all
}

// stepFileName matches the name of a step's file. Its first group is the
// number that orders the step, and its second is the suffix that chooses the
// step's kind.
var stepFileName = regexp.MustCompile(`^([0-9]{2})-.+\.(tx|stmt)\.sql$`)

// parseSteps returns the steps of a migration from files, which maps the name
// of each step's file to its contents, in the order in which they run.
//
// Each file's name must start with a two-digit number that no other file
// starts with, and the numbers order the steps. The name must end in ".tx.sql",
// for a [Transaction] step, or in ".stmt.sql", for a
// [Statement] step.
func parseSteps(files map[string]string) ([]Step, error) {
	var (
		steps  []Step
		prefix = map[string]string{}
	)

	for _, name := range slices.Sorted(maps.Keys(files)) {
		match := stepFileName.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf(
				"%q is not a step file: its name must start with a two-digit number and end in .tx.sql or .stmt.sql",
				name,
			)
		}

		if other, ok := prefix[match[1]]; ok {
			return nil, fmt.Errorf("%q and %q have the same number", other, name)
		}
		prefix[match[1]] = name

		kind := Transaction
		if match[2] == "stmt" {
			kind = Statement
		}

		steps = append(steps, Step{
			Name: name,
			Kind: kind,
			SQL:  files[name],
		})
	}

	return steps, nil
}
