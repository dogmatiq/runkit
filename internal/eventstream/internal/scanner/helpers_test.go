package scanner_test

import (
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	. "github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// expectToReceiveEnvelopes asserts that the given envelopes are received, in
// order, across one or more batches. It returns the checkpoint of the last
// batch received.
//
// It fails if any batch's checkpoint is lower than that of the batch before it.
func expectToReceiveEnvelopes(
	t testing.TB,
	batches <-chan Batch,
	want ...*envelopepb.Envelope,
) uint64 {
	t.Helper()

	var (
		got        []*envelopepb.Envelope
		checkpoint uint64
	)

	for len(got) < len(want) {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for envelopes: got %d of %d", len(got), len(want))
		case batch := <-batches:
			if batch.Checkpoint < checkpoint {
				t.Fatalf("checkpoint moved backwards: got %d, previously %d", batch.Checkpoint, checkpoint)
			}

			checkpoint = batch.Checkpoint
			got = append(got, batch.Envelopes...)
		}
	}

	xtesting.Expect(t, "envelopes", got, want)

	return checkpoint
}

// expectCheckpoint asserts that a batch with the given checkpoint is received
// without any intervening envelopes being delivered.
func expectCheckpoint(
	t testing.TB,
	batches <-chan Batch,
	want uint64,
) {
	t.Helper()

	for {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for checkpoint %d", want)
		case batch := <-batches:
			if len(batch.Envelopes) != 0 {
				t.Fatalf("unexpected envelopes in batch with checkpoint %d", batch.Checkpoint)
			}

			if batch.Checkpoint == want {
				return
			}

			if batch.Checkpoint > want {
				t.Fatalf("unexpected checkpoint: got %d, want %d", batch.Checkpoint, want)
			}
		}
	}
}

// expectNoBatches asserts that no batch is delivered within a short period.
func expectNoBatches(
	t testing.TB,
	batches <-chan Batch,
) {
	t.Helper()

	select {
	case batch := <-batches:
		t.Fatalf("unexpected batch with checkpoint %d and %d envelopes", batch.Checkpoint, len(batch.Envelopes))
	case <-time.After(100 * time.Millisecond):
	}
}
