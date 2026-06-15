package projection

import (
	"fmt"
	"log/slog"
	"time"
)

type scope struct {
	streamID                 string
	offset, checkpointOffset uint64
	recordedAt               time.Time
	logger                   *slog.Logger
}

func (s *scope) Now() time.Time {
	return time.Now()
}

func (s *scope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *scope) StreamID() string {
	return s.streamID
}

func (s *scope) Offset() uint64 {
	return s.offset
}

func (s *scope) CheckpointOffset() uint64 {
	return s.checkpointOffset
}

func (s *scope) RecordedAt() time.Time {
	return s.recordedAt
}
