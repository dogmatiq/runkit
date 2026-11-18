// Package current holds the database schema as it is at the target schema
// version: its tables, functions and triggers, grouped by the component of the
// engine that uses them.
package current

import "embed"

// FS holds the SQL files of the schema.
//
//go:embed *.sql
var FS embed.FS
