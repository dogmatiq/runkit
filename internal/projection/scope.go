package projection

import (
	"fmt"
	"log/slog"
	"time"
)

// messageScope implements [dogma.ProjectionEventScope].
type messageScope struct {
	streamID                 string
	offset, checkpointOffset uint64
	recordedAt               time.Time
	logger                   *slog.Logger
}

func (s *messageScope) Now() time.Time {
	return time.Now()
}

func (s *messageScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *messageScope) StreamID() string {
	return s.streamID
}

func (s *messageScope) Offset() uint64 {
	return s.offset
}

func (s *messageScope) CheckpointOffset() uint64 {
	return s.checkpointOffset
}

func (s *messageScope) RecordedAt() time.Time {
	return s.recordedAt
}

// compactScope implements [dogma.ProjectionCompactScope].
type compactScope struct {
	logger *slog.Logger
}

func (s *compactScope) Now() time.Time {
	return time.Now()
}

func (s *compactScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}
