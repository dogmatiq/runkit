// Package current holds the database schema as it is at the target schema
// version: its tables, functions and triggers, grouped by PostgreSQL schema.
package current

import "embed"

// FS holds the SQL files of the schema, one directory for each PostgreSQL
// schema. The pattern leaves out this package's Go files.
//
//go:embed */*
var FS embed.FS
