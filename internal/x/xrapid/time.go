package xrapid

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// Time returns a generator of random [time.Time] values.
func Time() *rapid.Generator[time.Time] {
	return rapid.Custom(
		func(t *rapid.T) time.Time {
			return time.Date(
				rapid.IntRange(2000, 2100).Draw(t, "year"),
				time.Month(rapid.IntRange(1, 12).Draw(t, "month")),
				rapid.IntRange(1, 31).Draw(t, "day"),
				rapid.IntRange(0, 23).Draw(t, "hour"),
				rapid.IntRange(0, 59).Draw(t, "minute"),
				rapid.IntRange(0, 59).Draw(t, "second"),
				rapid.IntRange(0, 999999999).Draw(t, "nanosecond"),
				time.UTC,
			)
		},
	)
}

// Timestamp returns a generator of random [*timestamppb.Timestamp] values.
func Timestamp() *rapid.Generator[*timestamppb.Timestamp] {
	return rapid.Custom(
		func(t *rapid.T) *timestamppb.Timestamp {
			return timestamppb.New(
				Time().Draw(t, "time"),
			)
		},
	)
}
