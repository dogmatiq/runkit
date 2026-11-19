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

// StepCanceledError is returned when PostgreSQL cancels a statement of a step.
//
// PostgreSQL cancels a statement in two cases, and reports both with the same
// error code, 57014 (query_canceled):
//
//   - The statement ran for longer than the statement timeout. Only a
//     [Transaction] step has a statement timeout, and only if the runner isn't
//     offline.
//   - Someone canceled the statement, such as an administrator who called
//     pg_cancel_backend().
//
// The error doesn't say which case happened, so the message covers both. The
// cases need different action. A step that ran past the timeout is likely to do
// so again, so it can't be applied while the application is running, and the
// operator must apply it offline. After a cancellation, the operator can apply
// the upgrade again.
type StepCanceledError struct {
	// MigratingTo is the schema version of the migration that the step belongs
	// to.
	MigratingTo version.Version

	// StepName is the name of the step.
	StepName string

	// Cause is the error that the database returned.
	Cause error
}

func (e StepCanceledError) Error() string {
	return fmt.Sprintf(
		"database schema migration canceled while applying step %s of the migration to %s: if PostgreSQL canceled it for taking too long, stop every instance of the application, then apply the upgrade offline; if someone canceled it, apply the upgrade again: %s",
		e.StepName,
		e.MigratingTo,
		e.Cause,
	)
}

// Unwrap returns the error that the database returned.
func (e StepCanceledError) Unwrap() error {
	return e.Cause
}

// StepFailedError is returned when a step of a migration fails, other than by
// PostgreSQL canceling one of its statements.
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
