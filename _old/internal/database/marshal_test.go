package database_test

import (
	"testing"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/proto"
)

func TestMarshalUUID(t *testing.T) {
	db, _ := databasetest.New(
		t,
		`CREATE TABLE test_uuid (
			id uuid PRIMARY KEY
		)`,
	)

	t.Run("it round-trips a UUID through a Postgres uuid column", func(t *testing.T) {
		original := uuidpb.Generate()

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO test_uuid (
				id
			) VALUES ($1)`,
			MarshalUUID(original),
		); err != nil {
			t.Fatal(err)
		}

		got := &uuidpb.UUID{}
		if err := db.QueryRowContext(
			t.Context(),
			`SELECT id
			FROM test_uuid
			WHERE id = $1`,
			MarshalUUID(original),
		).Scan(UnmarshalUUID(got)); err != nil {
			t.Fatal(err)
		}

		if got.AsString() != original.AsString() {
			t.Fatalf("unexpected result: got %q, want %q", got.AsString(), original.AsString())
		}
	})

	t.Run("it returns an error when scanning a non-string value", func(t *testing.T) {
		x := &uuidpb.UUID{}
		if err := UnmarshalUUID(x).Scan(12345); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("it returns an error when scanning an invalid UUID string", func(t *testing.T) {
		x := &uuidpb.UUID{}
		if err := UnmarshalUUID(x).Scan("not-a-uuid"); err == nil {
			t.Fatal("expected an error")
		}
	})
}

func TestMarshalEnvelope(t *testing.T) {
	db, _ := databasetest.New(
		t,
		`CREATE TABLE test_envelope (
			id serial PRIMARY KEY,
			data bytea NOT NULL
		)`,
	)

	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	t.Run("it round-trips an Envelope through a Postgres bytea column", func(t *testing.T) {
		original := packer.PackCommand(stubs.CommandA1)

		var id int
		if err := db.QueryRowContext(
			t.Context(),
			`INSERT INTO test_envelope (
				data
			) VALUES ($1) RETURNING id`,
			MarshalEnvelope(original),
		).Scan(&id); err != nil {
			t.Fatal(err)
		}

		got := &envelopepb.Envelope{}
		if err := db.QueryRowContext(
			t.Context(),
			`SELECT data
			FROM test_envelope
			WHERE id = $1`,
			id,
		).Scan(UnmarshalEnvelope(got)); err != nil {
			t.Fatal(err)
		}

		if !proto.Equal(original, got) {
			t.Fatalf("unexpected result: got %v, want %v", got, original)
		}
	})

	t.Run("it returns an error when scanning a non-byte-slice value", func(t *testing.T) {
		env := &envelopepb.Envelope{}
		if err := UnmarshalEnvelope(env).Scan("not bytes"); err == nil {
			t.Fatal("expected an error")
		}
	})
}
