package eventstream_test

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/reference-engine/internal/database"
	. "github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestAppend(t *testing.T) {
	db, _ := database.NewTestDB(t)
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

		gotNextOffset, err := Append(
			t.Context(),
			tx,
			envelopes,
		)
		if err != nil {
			t.Fatal(err)
		}

		if wantNextOffset := Offset(3); gotNextOffset != wantNextOffset {
			t.Fatalf("unexpected offset after last event: got %d, want %d", gotNextOffset, wantNextOffset)
		}

		for envelope := range envelopes.All() {
			SetOffset(envelope, Offset(len(wantEnvelopes)))
			wantEnvelopes = append(wantEnvelopes, envelope)
		}
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

		gotNextOffset, err := Append(
			t.Context(),
			tx,
			envelopes,
		)
		if err != nil {
			t.Fatal(err)
		}

		if wantNextOffset := Offset(5); gotNextOffset != wantNextOffset {
			t.Fatalf("unexpected offset after last event: got %d, want %d", gotNextOffset, wantNextOffset)
		}

		for envelope := range envelopes.All() {
			SetOffset(envelope, Offset(len(wantEnvelopes)))
			wantEnvelopes = append(wantEnvelopes, envelope)
		}
	}

	// Verify that we can read all events back in order, with no offset gaps.
	{
		wantOffset := Offset(0)
		wantOffsetMax := Offset(len(wantEnvelopes))

		for gotEnvelope, err := range Read(t.Context(), tx, 0) {
			if err != nil {
				t.Fatal(err)
			}

			if wantOffset > wantOffsetMax {
				t.Fatalf("unexpected offset during read: got %d, want less than %d", wantOffset, wantOffsetMax)
			}

			wantEnvelope := wantEnvelopes[wantOffset]
			xtesting.ExpectEnvelope(t, gotEnvelope, wantEnvelope)

			wantOffset++
		}

		if wantOffset != wantOffsetMax {
			t.Fatalf("unexpected number of events read: got %d, want %d", wantOffset, wantOffsetMax)
		}
	}
}

func TestRead(t *testing.T) {
	db, _ := database.NewTestDB(t)

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
			SetOffset(envelope, Offset(len(allEnvelopes)))
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

	t.Run("it yields each event in order", func(t *testing.T) {
		wantEnvelopes := allEnvelopes

		for gotEnvelope, err := range Read(t.Context(), tx, 0) {
			if err != nil {
				t.Fatal(err)
			}
			if len(wantEnvelopes) == 0 {
				t.Fatal("more events yielded than expected")
			}
			xtesting.ExpectEnvelope(t, gotEnvelope, wantEnvelopes[0])
			wantEnvelopes = wantEnvelopes[1:]
		}

		if len(wantEnvelopes) != 0 {
			t.Fatalf("fewer events yielded than expected: %d remaining", len(wantEnvelopes))
		}
	})

	t.Run("it skips events before the start offset", func(t *testing.T) {
		const startOffset = 10
		wantEnvelopes := allEnvelopes[startOffset:]

		for gotEnvelope, err := range Read(t.Context(), tx, startOffset) {
			if err != nil {
				t.Fatal(err)
			}
			if len(wantEnvelopes) == 0 {
				t.Fatal("more events yielded than expected")
			}
			xtesting.ExpectEnvelope(t, gotEnvelope, wantEnvelopes[0])
			wantEnvelopes = wantEnvelopes[1:]
		}

		if len(wantEnvelopes) != 0 {
			t.Fatalf("fewer events yielded than expected: %d remaining", len(wantEnvelopes))
		}
	})

	t.Run("it filters by aggregate instance ID", func(t *testing.T) {
		var wantEnvelopes []*envelopepb.Envelope
		for _, envelope := range allEnvelopes {
			src := envelope.GetHeader().GetSource()
			if src.GetHandler().GetKey().AsString() == aggregate1.GetKey().AsString() &&
				src.GetInstanceId() == "<instance-1>" {
				wantEnvelopes = append(wantEnvelopes, envelope)
			}
		}

		for gotEnvelope, err := range ReadForAggregateInstance(
			t.Context(),
			tx,
			0,
			aggregate1.GetKey(),
			"<instance-1>",
		) {
			if err != nil {
				t.Fatal(err)
			}
			if len(wantEnvelopes) == 0 {
				t.Fatal("more events yielded than expected")
			}
			xtesting.ExpectEnvelope(t, gotEnvelope, wantEnvelopes[0])
			wantEnvelopes = wantEnvelopes[1:]
		}

		if len(wantEnvelopes) != 0 {
			t.Fatalf("fewer events yielded than expected: %d remaining", len(wantEnvelopes))
		}
	})

	t.Run("it stops iteration when the caller breaks", func(t *testing.T) {
		count := 0
		for _, err := range Read(t.Context(), tx, 0) {
			if err != nil {
				t.Fatal(err)
			}
			count++
			break
		}
		if count != 1 {
			t.Fatalf("unexpected number of events: got %d, want 1", count)
		}
	})
}

