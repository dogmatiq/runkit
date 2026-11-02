package application_test

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/application"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestBind(t *testing.T) {
	t.Run("it binds a database that is not bound to an application", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		id := identitypb.New("<app>", uuidpb.Generate())

		if err := Bind(t.Context(), db, id); err != nil {
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

		if err := Bind(t.Context(), db, identitypb.New("<before>", key)); err != nil {
			t.Fatal(err)
		}

		if err := Bind(t.Context(), db, identitypb.New("<after>", key)); err != nil {
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

		if err := Bind(t.Context(), db, bound); err != nil {
			t.Fatal(err)
		}

		other := identitypb.New("<other>", uuidpb.Generate())

		err := Bind(t.Context(), db, other)
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

	t.Run("it returns an error if the schema has not been created", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)

		id := identitypb.New("<app>", uuidpb.Generate())

		if err := Bind(t.Context(), db, id); err == nil {
			t.Fatal("expected an error")
		}
	})
}
