package migrations

import (
	"fmt"

	"github.com/dogmatiq/runkit/internal/schema/internal/version"
)

// UnknownMigrationError is returned when the database has a migration in
// progress that isn't among the runner's migrations, so the runner can't finish
// that migration.
type UnknownMigrationError struct {
	// MigratingTo is the schema version of the migration in progress.
	MigratingTo version.Version
}

func (e UnknownMigrationError) Error() string {
	return fmt.Sprintf(
		"incompatible database schema: the database has an in-progress migration to %s, which this runkit version doesn't include; it may come from a hotfix to a prior runkit version",
		e.MigratingTo,
	)
}

// InvalidProgressError is returned when the number of completed steps recorded
// for the migration in progress is higher than the number of steps that the
// migration has.
type InvalidProgressError struct {
	// MigratingTo is the schema version of the migration in progress.
	MigratingTo version.Version

	// StepsCompleted is the number of completed steps recorded in the database.
	StepsCompleted int

	// StepCount is the number of steps that the migration has.
	StepCount int
}

func (e InvalidProgressError) Error() string {
	return fmt.Sprintf(
		"invalid migration progress in the database: the number of completed steps is %d, but the migration to %s only has %d step(s)",
		e.StepsCompleted,
		e.MigratingTo,
		e.StepCount,
	)
}

// StepTimeoutError is returned when a step runs for longer than the statement
// timeout, which applies unless the runner is offline.
type StepTimeoutError struct {
	// MigratingTo is the schema version of the migration that the step belongs
	// to.
	MigratingTo version.Version

	// StepName is the name of the step.
	StepName string

	// Cause is the error that the database returned.
	Cause error
}

func (e StepTimeoutError) Error() string {
	return fmt.Sprintf(
		"database schema migration timed out while applying step %s of the migration to %s: the step takes longer than is allowed while the application is running, so stop every instance of the application, then apply the upgrade offline: %s",
		e.StepName,
		e.MigratingTo,
		e.Cause,
	)
}

// Unwrap returns the error that the database returned.
func (e StepTimeoutError) Unwrap() error {
	return e.Cause
}

// StepFailedError is returned when a step of a migration fails, other than by
// running for longer than the statement timeout.
type StepFailedError struct {
	// MigratingTo is the schema version of the migration that the step belongs
	// to.
	MigratingTo version.Version

	// StepName is the name of the step.
	StepName string

	// Cause is the error that stopped the step.
	Cause error
}

func (e StepFailedError) Error() string {
	return fmt.Sprintf(
		"unable to apply step %s of the migration to schema version %s: %s",
		e.StepName,
		e.MigratingTo,
		e.Cause,
	)
}

// Unwrap returns the error that stopped the step.
func (e StepFailedError) Unwrap() error {
	return e.Cause
}
