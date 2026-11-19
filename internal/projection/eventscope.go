package projection

import (
	"fmt"
	"log/slog"
	"time"
	"uuid"
)

// eventScope implements [dogma.ProjectionEventScope].
type eventScope struct {
	messageID                uuid.UUID
	streamID                 uuid.UUID
	offset, checkpointOffset uint64
	recordedAt               time.Time
	engineLogger             *slog.Logger
	appLogger                *slog.Logger
}

func (s *eventScope) Now() time.Time {
	return time.Now()
}

func (s *eventScope) Log(format string, args ...any) {
	text := fmt.Sprintf(format, args...)

	s.engineLogger.Debug(
		"handler logged a message",
		slog.String("text", text),
	)

	s.appLogger.Info(text)
}

func (s *eventScope) MessageID() uuid.UUID {
	return s.messageID
}

func (s *eventScope) StreamID() uuid.UUID {
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
