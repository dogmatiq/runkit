package projection

import (
	"fmt"
	"log/slog"
	"time"
)

// eventScope implements [dogma.ProjectionEventScope].
type eventScope struct {
	streamID         string
	offset           uint64
	checkpointOffset uint64
	recordedAt       time.Time
	logger           *slog.Logger
}

func (s *eventScope) StreamID() string {
	return s.streamID
}

func (s *eventScope) Offset() uint64 {
	return s.offset
}

func (s *eventScope) CheckpointOffset() uint64 {
	return s.checkpointOffset
}

func (s *eventScope) RecordedAt() time.Time {
	return s.recordedAt
}

func (s *eventScope) Now() time.Time {
	return time.Now()
}

func (s *eventScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
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
