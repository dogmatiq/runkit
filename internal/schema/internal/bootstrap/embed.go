// Package bootstrap holds the SQL that creates the table that stores the schema
// version of the database.
package bootstrap

import _ "embed"

// SQL creates the table that stores the schema version of the database.
//
// It runs before any migration, so no migration can change what it creates.
//
//go:embed bootstrap.sql
var SQL string
