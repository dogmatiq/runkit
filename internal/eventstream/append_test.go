package eventstream_test

import (
	"database/sql"
	"slices"
	"testing"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	. "github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestAppend(t *testing.T) {
	db := xtesting.NewDatabase(t)

	xtesting.Transact(
		t,
		db,
		func(tx *sql.Tx) {
			streamIDs := xtesting.CreateEventStreams(t, tx, 1)

			envelopePacker := &envelopepb.Packer{
				Application: identitypb.New("<app>", uuidpb.Generate()),
			}

			effectPacker := envelopePacker.PackEffects(
				envelopePacker.PackCommand(stubs.CommandA1),
				identitypb.New("<handler>", uuidpb.Generate()),
			)

			effectPacker.PackEvent(stubs.EventA1)
			effectPacker.PackEvent(stubs.EventA2)

			eventEnvelopes, _ := effectPacker.Seal()

			got, err := eventstream.Append(
				t.Context(),
				tx,
				streamIDs[0],
				eventEnvelopes,
			)
			if err != nil {
				t.Fatal(err)
			}

			if want := Offset(2); got != want {
				t.Fatalf("unexpected offset: got %d, want %d", got, want)
			}

			xtesting.ExpectContiguousEventEnvelopes(
				t,
				tx,
				streamIDs[0],
				0,
				slices.Collect(eventEnvelopes.All())...,
			)
		},
	)
}
