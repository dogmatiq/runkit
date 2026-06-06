package eventstream_test

import (
	"context"
	"fmt"
	"iter"
	"testing"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/database/databasetest"
	. "github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestEventStream(t *testing.T) {
	db, _ := databasetest.New(t)

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	eventStreamID, err := Acquire(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}

	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	// Create handler identities for the events we will pack.
	var (
		integration1 = identitypb.MustParse("<integration-1>", "ef5ea913-f312-422f-9c40-32280c17ac04")
		integration2 = identitypb.MustParse("<integration-2>", "361f6bf4-5705-4ce5-8cfb-f83d416d8bea")
		aggregate1   = identitypb.MustParse("<aggregate-1>", "9e2b2327-73c0-47e1-a5c5-f97cc3d759a1")
		aggregate2   = identitypb.MustParse("<aggregate-2>", "4f3a8c12-2d61-4a8e-b3f0-7c9e2a4b5d68")
	)

	// commonCommand is a command recorded as the cause of events from multiple
	// batches so we can test filtering by correlation ID.
	commonCommand := packer.PackCommand(stubs.CommandZ1)

	// Describe the event stream we'll build using batches of events from the
	// handlers above.
	eventBatches := []struct {
		Cause      *envelopepb.Envelope
		Handler    *identitypb.Identity
		Instance   string
		EventCount int
	}{
		{
			Cause:      packer.PackCommand(stubs.CommandA1),
			Handler:    integration1,
			EventCount: 1,
		},
		{
			Cause:      commonCommand,
			Handler:    aggregate1,
			Instance:   "<instance-1>",
			EventCount: 75,
		},
		{
			Cause:      packer.PackCommand(stubs.CommandB1),
			Handler:    integration2,
			EventCount: 2,
		},
		{
			Cause:      packer.PackCommand(stubs.CommandC1),
			Handler:    aggregate2,
			Instance:   "<instance-1>",
			EventCount: 10,
		},
		{
			Cause:      packer.PackCommand(stubs.CommandD1),
			Handler:    aggregate1,
			Instance:   "<instance-2>",
			EventCount: 1,
		},
		{
			Cause:      commonCommand,
			Handler:    integration1,
			EventCount: 1,
		},
		{
			Cause:      packer.PackCommand(stubs.CommandE1),
			Handler:    aggregate1,
			Instance:   "<instance-1>",
			EventCount: 75,
		},
	}

	var allEnvelopes []*envelopepb.Envelope

	for _, batch := range eventBatches {
		var opts []envelopepb.PackEffectsOption
		if batch.Instance != "" {
			opts = append(opts, envelopepb.WithInstanceID(batch.Instance))
		}

		p := packer.PackEffects(
			batch.Cause,
			batch.Handler,
			opts...,
		)

		for range batch.EventCount {
			p.PackEvent(stubs.EventA1)
		}

		envelopes, _ := p.Seal()

		for envelope := range envelopes.All() {
			envelopepb.SetExtension(
				envelope.GetBody(),
				envelopepb.
					NewEventStreamPositionBuilder().
					WithStreamId(eventStreamID).
					WithOffset(uint64(len(allEnvelopes))).
					Build(),
			)

			allEnvelopes = append(allEnvelopes, envelope)
		}

		wantNextOffset := Offset(len(allEnvelopes))
		gotNextOffset, err := Append(t.Context(), tx, eventStreamID, envelopes)
		if err != nil {
			t.Fatal(err)
		}

		if gotNextOffset != wantNextOffset {
			t.Fatalf(
				"unexpected next offset after appending batch: got %d, want %d",
				gotNextOffset,
				wantNextOffset,
			)
		}
	}

	if len(allEnvelopes) <= EventsPerPage {
		panic(fmt.Sprintf(
			"not enough events to test pagination: got %d, want more than %d",
			len(allEnvelopes),
			EventsPerPage,
		))
	}

	cases := []struct {
		Name   string
		Read   func(context.Context, database.Executor, *uuidpb.UUID, Offset) iter.Seq2[*envelopepb.Envelope, error]
		Filter func(envelope *envelopepb.Envelope) bool
	}{
		{
			Name: "func Read()",
			Read: func(
				ctx context.Context,
				q database.Executor,
				eventStreamID *uuidpb.UUID,
				offset Offset,
			) iter.Seq2[*envelopepb.Envelope, error] {
				return Read(ctx, q, eventStreamID, offset, nil)
			},
			Filter: func(envelope *envelopepb.Envelope) bool {
				return true
			},
		},
		{
			Name: "func ReadByAggregateInstance()",
			Read: func(
				ctx context.Context,
				q database.Executor,
				eventStreamID *uuidpb.UUID,
				offset Offset,
			) iter.Seq2[*envelopepb.Envelope, error] {
				return ReadByAggregateInstance(
					ctx,
					q,
					eventStreamID,
					offset,
					aggregate1.GetKey(),
					"<instance-1>",
				)
			},
			Filter: func(envelope *envelopepb.Envelope) bool {
				src := envelope.GetHeader().GetSource()

				if !src.GetHandler().GetKey().Equal(aggregate1.GetKey()) {
					return false
				}

				if src.GetInstanceId() != "<instance-1>" {
					return false
				}

				return true
			},
		},
		{
			Name: "func ReadByCorrelationID()",
			Read: func(
				ctx context.Context,
				q database.Executor,
				eventStreamID *uuidpb.UUID,
				offset Offset,
			) iter.Seq2[*envelopepb.Envelope, error] {
				return ReadByCorrelationID(
					ctx,
					q,
					eventStreamID,
					offset,
					nil,
					commonCommand.GetHeader().GetCorrelationId(),
				)
			},
			Filter: func(envelope *envelopepb.Envelope) bool {
				return envelope.GetHeader().GetCorrelationId().Equal(
					commonCommand.GetHeader().GetCorrelationId(),
				)
			},
		},
	}

	filterEnvelopes := func(
		envelopes []*envelopepb.Envelope,
		fn func(envelope *envelopepb.Envelope) bool,
	) (filtered []*envelopepb.Envelope) {
		for _, envelope := range envelopes {
			if fn(envelope) {
				filtered = append(filtered, envelope)
			}
		}
		return filtered
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			t.Run("it yields each event in order", func(t *testing.T) {
				xtesting.ExpectEventEnvelopes(
					t,
					c.Read(
						t.Context(),
						tx,
						eventStreamID,
						0,
					),
					filterEnvelopes(
						allEnvelopes,
						c.Filter,
					)...,
				)
			})

			t.Run("it skips events before the start offset", func(t *testing.T) {
				const startOffset = 10

				xtesting.ExpectEventEnvelopes(
					t,
					c.Read(
						t.Context(),
						tx,
						eventStreamID,
						startOffset,
					),
					filterEnvelopes(
						allEnvelopes[startOffset:],
						c.Filter,
					)...,
				)
			})
		})
	}

}
