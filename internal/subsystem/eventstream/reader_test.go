package eventstream_test

import (
	"fmt"

	"github.com/dogmatiq/dapper"
	. "github.com/dogmatiq/runkit/internal/subsystem/eventstream"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func (s *state) ReadFromStream(t *rapid.T) {
	part := s.stream.PartitionsGen(t).Draw(t, "stream partition")
	wantOffset := part.OffsetsGen(t).Draw(t, "start offset")

	r, err := NewReader(
		t.Context(),
		&s.stream.Journals.NonFailing,
		part.ID,
		wantOffset,
	)
	if err != nil {
		t.Fatalf("unable to create reader for partition %s: %s", part, err)
	}
	defer r.Close()

	for _, wantEnv := range part.Events[wantOffset:] {
		gotOffset, gotEnv, ok, err := r.Read(t.Context())
		if err != nil {
			t.Fatalf(
				"unable to read from partition %s at offset %d: %s",
				part,
				wantOffset,
				err,
			)
		}

		if !ok {
			t.Fatalf(
				"unexpected end of partition %s at offset %d",
				part,
				gotOffset,
			)
		}

		if gotOffset != wantOffset {
			t.Fatalf(
				"unexpected offset when reading event from partition %s: got %d, want %d",
				part,
				gotOffset,
				wantOffset,
			)
		}

		if !gotEnv.MessageId.Equal(wantEnv.MessageId) {
			desc := "which is not in the stream"
			if foundAtOffset, ok := part.FindOffset(gotEnv.MessageId); ok {
				desc = fmt.Sprintf("which is actually at offset %d", foundAtOffset)
			}

			t.Fatalf(
				"unexpected event at offset %d of partition %s: got %s (%s), want %s",
				gotOffset,
				part,
				gotEnv.MessageId,
				desc,
				wantEnv.MessageId,
			)
		}

		if !proto.Equal(gotEnv, wantEnv) {
			t.Fatalf(
				"unexpected envelope at offset %d of partition %s:\ngot %s\nwant %s",
				gotOffset,
				part,
				dapper.Format(gotEnv),
				dapper.Format(wantEnv),
			)
		}

		t.Logf(
			"read expected event at offset %d of partition %s: %s",
			gotOffset,
			part,
			gotEnv.MessageId,
		)

		wantOffset++
	}

	gotOffset, gotEnv, ok, err := r.Read(t.Context())
	if err != nil {
		t.Fatalf(
			"unable to read from partition %s at offset %d: %s",
			part,
			wantOffset,
			err,
		)
	}

	if !ok {
		// Expected the end, all good!
		return
	}

	desc := "which is not in the stream"
	if foundAtOffset, ok := part.FindOffset(gotEnv.MessageId); ok {
		desc = fmt.Sprintf("which is actually @%d", foundAtOffset)
	}

	t.Fatalf(
		"read unexpected event at offset %d of partition %s: got %s (%s), want end-of-stream",
		gotOffset,
		part,
		gotEnv.MessageId,
		desc,
	)
}
