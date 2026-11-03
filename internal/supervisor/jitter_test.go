package supervisor_test

import (
	"testing"
	"time"

	. "github.com/dogmatiq/runkit/internal/supervisor"
	"pgregory.net/rapid"
)

func TestJitter(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		d := time.Duration(rapid.Int64Range(2, int64(time.Hour)).Draw(t, "d"))

		got := Jitter(d)

		if got < d/2 || got > d {
			t.Fatalf("unexpected duration: got %s, want between %s and %s", got, d/2, d)
		}
	})
}
