package migrations

import "github.com/dogmatiq/runkit/internal/schema/internal/version"

// Kind is the kind of a [Step].
type Kind int

const (
	// Transaction is the kind of a step that holds statements which run
	// together in one transaction.
	Transaction Kind = iota + 1

	// Statement is the kind of a step that holds one statement, which runs on
	// its own outside any transaction.
	//
	// The statement and the record of the step commit separately, so a failure
	// between them runs the statement again. It must therefore be safe to run
	// twice, such as by using IF NOT EXISTS.
	Statement
)

// Step is one unit of work in a [Migration].
type Step struct {
	// Name is how an error message refers to the step.
	Name string

	// Kind is the kind of step.
	Kind Kind

	// SQL is the SQL that the step runs.
	SQL string
}

// Migration is a list of steps that brings a database to a schema version.
type Migration struct {
	// Version is the schema version that the migration produces. The database
	// stores it to record the migration whose steps are being applied.
	Version version.Version

	// Steps are the steps of the migration, in the order in which they run.
	Steps []Step
}
