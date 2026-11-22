package cluster_test

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

func TestJoin(t *testing.T) {
	t.Run("it creates the cluster of a database that has none", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		app := identitypb.New("<app>", uuidpb.Generate())

		id, err := Join(t.Context(), db, app)
		if err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"stored cluster",
			true,
			db,
			`SELECT id = $1
				AND application_key = $2
				AND application_name = '<app>'
			FROM runkit.cluster`,
			id.String(),
			app.GetKey().AsStd().String(),
		)
	})

	t.Run("it returns the same ID every time that it is called for a database", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		app := identitypb.New("<app>", uuidpb.Generate())

		first, err := Join(t.Context(), db, app)
		if err != nil {
			t.Fatal(err)
		}

		second, err := Join(t.Context(), db, app)
		if err != nil {
			t.Fatal(err)
		}

		if first != second {
			t.Fatalf("unexpected cluster ID: got %s, want %s", second, first)
		}
	})

	t.Run("it returns different IDs for different databases", func(t *testing.T) {
		app := identitypb.New("<app>", uuidpb.Generate())

		db1 := xtesting.NewDatabase(t)
		db2 := xtesting.NewDatabase(t)

		first, err := Join(t.Context(), db1, app)
		if err != nil {
			t.Fatal(err)
		}

		second, err := Join(t.Context(), db2, app)
		if err != nil {
			t.Fatal(err)
		}

		if first == second {
			t.Fatalf("both databases have the cluster ID %s", first)
		}
	})

	t.Run("it updates the name of the application that the cluster runs", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		key := uuidpb.Generate()

		before := identitypb.New("<before>", key)
		after := identitypb.New("<after>", key)

		if _, err := Join(t.Context(), db, before); err != nil {
			t.Fatal(err)
		}

		if _, err := Join(t.Context(), db, after); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectQueryResult(
			t,
			"stored application name",
			"<after>",
			db,
			`SELECT application_name
			FROM runkit.cluster`,
		)
	})

	t.Run("it returns an error if the cluster runs a different application", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		running := identitypb.New("<running>", uuidpb.Generate())

		if _, err := Join(t.Context(), db, running); err != nil {
			t.Fatal(err)
		}

		other := identitypb.New("<other>", uuidpb.Generate())

		_, err := Join(t.Context(), db, other)
		if err == nil {
			t.Fatal("expected an error")
		}

		want := fmt.Sprintf(
			"it already runs application %s",
			running,
		)
		if err.Error() != want {
			t.Fatalf("unexpected error: got %q, want %q", err, want)
		}

		xtesting.ExpectQueryResult(
			t,
			"stored application name",
			"<running>",
			db,
			`SELECT application_name
			FROM runkit.cluster`,
		)
	})

	t.Run("the database refuses a second cluster", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		app := identitypb.New("<app>", uuidpb.Generate())

		if _, err := Join(t.Context(), db, app); err != nil {
			t.Fatal(err)
		}

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO runkit.cluster (
				id,
				application_key,
				application_name
			) VALUES (
				gen_random_uuid(),
				gen_random_uuid(),
				'<second>'
			)`,
		); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error if the schema has not been created", func(t *testing.T) {
		db := xtesting.NewDatabaseWithoutSchema(t)
		app := identitypb.New("<app>", uuidpb.Generate())

		if _, err := Join(t.Context(), db, app); err == nil {
			t.Fatal("expected an error")
		}
	})
}
