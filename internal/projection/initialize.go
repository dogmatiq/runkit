package projection

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// InitializeHandler initializes the state of a projection message handler.
//
// It is called by the engine on startup, before any components are started.
func InitializeHandler(
	ctx context.Context,
	db *sql.DB,
	handlerConfig *config.Projection,
) error {
	handlerKey := handlerConfig.Identity().GetKey()

	if _, err := db.ExecContext(
		ctx,
		`INSERT INTO projection.handlers (
			handler_key
		)
		VALUES ($1)
		ON CONFLICT (handler_key) DO NOTHING`,
		xsql.UUID(handlerKey),
	); err != nil {
		return fmt.Errorf("unable to insert handler: %w", err)
	}

	return nil
}
