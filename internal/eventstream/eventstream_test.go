package eventstream_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	. "github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/pgtest"
	"google.golang.org/protobuf/proto"
)

func TestAppend(t *testing.T) {
	db, _ := pgtest.Setup(t)
	packer := &envelopepb.Packer{
		Application: identitypb.MustParse("<app>", "7803a1f8-cfe2-47c1-bbee-610ec37b6008"),
	}

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	var wantEnvelopes []*envelopepb.Envelope

	// Pack some events produced by <handler-1>.
	{
		p := packer.PackEffects(
			packer.PackCommand(stubs.CommandA1),
			identitypb.MustParse("<handler-1>", "ef5ea913-f312-422f-9c40-32280c17ac04"),
		)
		p.PackEvent(stubs.EventA1)
		p.PackEvent(stubs.EventA2)
		p.PackEvent(stubs.EventA3)
		envelopes, _ := p.Seal()

		wantOffset := Offset(3)
		gotOffset, err := Append(
			t.Context(),
			tx,
			envelopes,
		)
		if err != nil {
			t.Fatal(err)
		}

		if gotOffset != wantOffset {
			t.Fatalf("unexpected offset after last event: got %d, want %d", gotOffset, wantOffset)
		}

		wantEnvelopes = slices.Collect(envelopes.All())
	}

	// Pack some events produced by <handler-2>.
	{
		p := packer.PackEffects(
			packer.PackCommand(stubs.CommandA2),
			identitypb.MustParse("<handler-2>", "9e2b2327-73c0-47e1-a5c5-f97cc3d759a1"),
		)
		p.PackEvent(stubs.EventA1)
		p.PackEvent(stubs.EventA2)
		envelopes, _ := p.Seal()

		wantOffset := Offset(5)
		gotOffset, err := Append(
			t.Context(),
			tx,
			envelopes,
		)
		if err != nil {
			t.Fatal(err)
		}

		if gotOffset != wantOffset {
			t.Fatalf("unexpected offset after last event: got %d, want %d", gotOffset, wantOffset)
		}

		for envelope := range envelopes.All() {
			wantEnvelopes = append(wantEnvelopes, envelope)
		}
	}

	// Verify that we can read all events back in order, with no offset gaps.
	{
		wantOffset := Offset(0)

		if err := Read(
			t.Context(),
			tx,
			0,
			"", "", // no filter
			func(gotEv Event) error {
				if gotEv.Offset != wantOffset {
					t.Fatalf("unexpected offset during read: got %d, want %d", gotEv.Offset, wantOffset)
				}

				wantEnv := wantEnvelopes[gotEv.Offset]
				if !proto.Equal(gotEv.Envelope, wantEnv) {
					t.Fatalf("unexpected envelope at offset %d: got %v, want %v", gotEv.Offset, gotEv.Envelope, wantEnv)
				}

				wantOffset++

				return nil
			},
		); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRead(t *testing.T) {
	db, _ := pgtest.Setup(t)

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

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

	// Describe the event stream we'll build using batches of events from the
	// handlers above.
	eventBatches := []struct {
		Handler    *identitypb.Identity
		Instance   string
		EventCount int
	}{
		{
			Handler:    integration1,
			EventCount: 1,
		},
		{
			Handler:    aggregate1,
			Instance:   "<instance-1>",
			EventCount: 75,
		},
		{
			Handler:    integration2,
			EventCount: 2,
		},
		{
			Handler:    aggregate2,
			Instance:   "<instance-1>",
			EventCount: 10,
		},
		{
			Handler:    aggregate1,
			Instance:   "<instance-2>",
			EventCount: 1,
		},
		{
			Handler:    integration1,
			EventCount: 1,
		},
		{
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
			packer.PackCommand(stubs.CommandA1),
			batch.Handler,
			opts...,
		)
		for range batch.EventCount {
			p.PackEvent(stubs.EventA1)
		}
		envelopes, _ := p.Seal()

		if _, err := Append(t.Context(), tx, envelopes); err != nil {
			t.Fatal(err)
		}

		for envelope := range envelopes.All() {
			allEnvelopes = append(allEnvelopes, envelope)
		}
	}

	if len(allEnvelopes) <= EventsPerPage {
		panic(fmt.Sprintf(
			"not enough events to test pagination: got %d, want more than %d",
			len(allEnvelopes),
			EventsPerPage,
		))
	}

	t.Run("it calls fn for each event", func(t *testing.T) {
		wantOffset := Offset(0)
		wantEnvelopes := allEnvelopes

		if err := Read(
			t.Context(),
			tx,
			0,
			"", "", // no filter
			func(gotEv Event) error {
				if gotEv.Offset != wantOffset {
					t.Fatalf("unexpected offset during read: got %d, want %d", gotEv.Offset, wantOffset)
				}

				wantEnv := wantEnvelopes[0]
				if !proto.Equal(gotEv.Envelope, wantEnv) {
					t.Fatalf("unexpected envelope at offset %d: got %v, want %v", gotEv.Offset, gotEv.Envelope, wantEnv)
				}

				wantEnvelopes = wantEnvelopes[1:]
				wantOffset++

				return nil
			},
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it skips events before the start offset", func(t *testing.T) {
		const startOffset = 10
		wantOffset := Offset(startOffset)
		wantEnvelopes := allEnvelopes[startOffset:]

		if err := Read(
			t.Context(),
			tx,
			startOffset,
			"", "", // no filter
			func(gotEv Event) error {
				if gotEv.Offset != wantOffset {
					t.Fatalf("unexpected offset during read: got %d, want %d", gotEv.Offset, wantOffset)
				}

				wantEnv := wantEnvelopes[0]
				if !proto.Equal(gotEv.Envelope, wantEnv) {
					t.Fatalf("unexpected envelope at offset %d: got %v, want %v", gotEv.Offset, gotEv.Envelope, wantEnv)
				}

				wantEnvelopes = wantEnvelopes[1:]
				wantOffset++

				return nil
			},
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it filters by aggregate instance ID", func(t *testing.T) {
		var (
			wantOffsets   []Offset
			wantEnvelopes []*envelopepb.Envelope
		)

		for offset, envelope := range allEnvelopes {
			src := envelope.GetHeader().GetSource()
			if src.GetHandler().GetKey().AsString() == aggregate1.GetKey().AsString() &&
				src.GetInstanceId() == "<instance-1>" {
				wantOffsets = append(wantOffsets, Offset(offset))
				wantEnvelopes = append(wantEnvelopes, envelope)
			}
		}

		if err := Read(
			t.Context(),
			tx,
			0,
			aggregate1.GetKey().AsString(),
			"<instance-1>",
			func(gotEv Event) error {
				wantOffset := wantOffsets[0]
				if gotEv.Offset != wantOffset {
					t.Fatalf("unexpected offset during read: got %d, want %d", gotEv.Offset, wantOffset)
				}

				wantEnv := wantEnvelopes[0]
				if !proto.Equal(gotEv.Envelope, wantEnv) {
					t.Fatalf("unexpected envelope at offset %d: got %v, want %v", gotEv.Offset, gotEv.Envelope, wantEnv)
				}

				wantOffsets = wantOffsets[1:]
				wantEnvelopes = wantEnvelopes[1:]

				return nil
			},
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it returns immediately if fn returns an error", func(t *testing.T) {
		called := false
		wantErr := errors.New("<error>")
		gotErr := Read(
			t.Context(),
			tx,
			0,
			"", "", // no filter
			func(Event) error {
				if called {
					t.Fatal("fn called more than once")
				}
				called = true
				return wantErr
			},
		)
		if gotErr != wantErr {
			t.Fatalf("unexpected error: got %v, want %v", gotErr, wantErr)
		}
	})
}
