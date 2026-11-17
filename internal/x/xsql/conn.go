package xsql

import (
	"database/sql"
	"database/sql/driver"
)

// Discard closes the connection instead of returning it to its pool, which
// releases any lock that its session holds.
func Discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
}
