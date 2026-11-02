package application_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/application"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestBind(t *testing.T) {
	t.Run("it binds a database that is not bound to an application", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		id := identitypb.New("<app>", uuidpb.Generate())

		if err := bind(t, db, id); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"bound application name",
			"<app>",
			db,
			`SELECT name
			FROM application.identity
			WHERE key = $1`,
			id.GetKey().AsString(),
		)
	})

	t.Run("it updates the name of the bound application", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		key := uuidpb.Generate()

		if err := bind(t, db, identitypb.New("<before>", key)); err != nil {
			t.Fatal(err)
		}

		if err := bind(t, db, identitypb.New("<after>", key)); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"bound application name",
			"<after>",
			db,
			`SELECT name
			FROM application.identity`,
		)
	})

	t.Run("it returns an error if the database is bound to a different application", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		bound := identitypb.New("<bound>", uuidpb.Generate())

		if err := bind(t, db, bound); err != nil {
			t.Fatal(err)
		}

		other := identitypb.New("<other>", uuidpb.Generate())

		err := bind(t, db, other)
		if err == nil {
			t.Fatal("expected an error")
		}

		want := fmt.Sprintf(
			"it is already bound to application %s",
			bound,
		)
		if err.Error() != want {
			t.Fatalf("unexpected error: got %q, want %q", err, want)
		}

		xtesting.ExpectQueryResult(
			t,
			"bound application name",
			"<bound>",
			db,
			`SELECT name
			FROM application.identity`,
		)
	})

	t.Run("it retries until ctx is canceled", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)

		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()

		err := Bind(
			ctx,
			db,
			identitypb.New("<app>", uuidpb.Generate()),
			spruce.NewTestLogger(t),
		)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected error: got %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

// bind calls [Bind] with the test's context and logger.
func bind(t *testing.T, db *sql.DB, id *identitypb.Identity) error {
	return Bind(
		t.Context(),
		db,
		id,
		spruce.NewTestLogger(t),
	)
}
