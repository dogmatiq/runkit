package eventstream_test

import (
	"fmt"

	"github.com/dogmatiq/dapper"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func (s *state) ReadFromStream(t *rapid.T) {
	stream := s.subsystem.StreamsGen().Draw(t, "stream")
	wantOffset := stream.OffsetsGen().Draw(t, "start offset")

	r, err := NewReader(
		t.Context(),
		&s.subsystem.Journals,
		stream.ID,
		wantOffset,
	)
	if err != nil {
		t.Fatalf("[%s] unable to create reader: %s", stream, err)
	}

	for _, wantEnv := range stream.Events[wantOffset:] {
		gotOffset, gotEnv, ok, err := r.Read(t.Context())
		if err != nil {
			t.Fatalf(
				"[%s] unable to read @%d: %s",
				stream,
				wantOffset,
				err,
			)
		}

		if !ok {
			t.Fatalf(
				"[%s] unexpected end @%d",
				stream,
				gotOffset,
			)
		}

		if gotOffset != wantOffset {
			t.Fatalf(
				"[%s] unexpected offset when reading event: got %d, want %d",
				stream,
				gotOffset,
				wantOffset,
			)
		}

		if !gotEnv.MessageId.Equal(wantEnv.MessageId) {
			desc := "which is not in the stream"
			if foundAtOffset, ok := stream.OffsetOf(gotEnv.MessageId); ok {
				desc = fmt.Sprintf("which is actually @%d", foundAtOffset)
			}

			t.Fatalf(
				"[%s] unexpected event @%d: got %s (%s), want %s",
				stream,
				gotOffset,
				gotEnv.MessageId,
				desc,
				wantEnv.MessageId,
			)
		}

		if !proto.Equal(gotEnv, wantEnv) {
			t.Fatalf(
				"[%s] unexpected envelope @%d:\ngot %s\nwant %s",
				stream,
				gotOffset,
				dapper.Format(gotEnv),
				dapper.Format(wantEnv),
			)
		}

		t.Logf(
			"[%s] read expected event @%d: %s",
			stream,
			gotOffset,
			gotEnv.MessageId,
		)

		wantOffset++
	}

	gotOffset, gotEnv, ok, err := r.Read(t.Context())
	if err != nil {
		t.Fatalf(
			"[%s] unable to read @%d: %s",
			stream,
			wantOffset,
			err,
		)
	}

	if !ok {
		// Expected the end, all good!
		return
	}

	desc := "which is not in the stream"
	if foundAtOffset, ok := stream.OffsetOf(gotEnv.MessageId); ok {
		desc = fmt.Sprintf("which is actually @%d", foundAtOffset)
	}

	t.Fatalf(
		"[%s] read unexpected event @%d: got %s (%s), want end-of-stream",
		stream,
		gotOffset,
		gotEnv.MessageId,
		desc,
	)
}
