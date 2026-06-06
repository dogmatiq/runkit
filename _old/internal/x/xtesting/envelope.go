package xtesting

import (
	"testing"

	"github.com/dogmatiq/dapper"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"google.golang.org/protobuf/proto"
)

// ExpectEnvelope compares the given envelopes and fails the test if they are
// not equal.
func ExpectEnvelope(t testing.TB, got, want *envelopepb.Envelope) {
	t.Helper()

	if proto.Equal(got, want) {
		return
	}

	t.Logf("unexpected envelope")
	t.Logf("+++ got:\n%s", dapper.Format(got))
	t.Logf("--- want:\n%s", dapper.Format(want))
	t.FailNow()
}
