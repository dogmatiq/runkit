package xslog

import (
	"context"
	"log/slog"
	"sync"
)

// Buffered returns a logger that holds the records written to it until the
// returned function is called.
//
// The function writes the records to the given logger, in the order they were
// made, keeping each record's time and the attributes and groups added to the
// logger that made it. Loggers derived from the returned logger share its
// records.
func Buffered(logger *slog.Logger) (*slog.Logger, func(context.Context)) {
	records := &bufferedRecords{}

	h := &bufferHandler{
		target:  logger.Handler(),
		records: records,
	}

	return slog.New(h), records.Flush
}

// bufferHandler is an [slog.Handler] that holds records until they are
// flushed.
type bufferHandler struct {
	target  slog.Handler
	records *bufferedRecords
}

func (h *bufferHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.target.Enabled(ctx, level)
}

func (h *bufferHandler) Handle(_ context.Context, r slog.Record) error {
	h.records.Add(h.target, r.Clone())
	return nil
}

func (h *bufferHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &bufferHandler{
		target:  h.target.WithAttrs(attrs),
		records: h.records,
	}
}

func (h *bufferHandler) WithGroup(name string) slog.Handler {
	return &bufferHandler{
		target:  h.target.WithGroup(name),
		records: h.records,
	}
}

// bufferedRecords is the set of records held by a buffered logger and the
// loggers derived from it.
type bufferedRecords struct {
	m       sync.Mutex
	records []bufferedRecord
}

// bufferedRecord is a held record, with the handler that writes it when it is
// flushed.
type bufferedRecord struct {
	Handler slog.Handler
	Record  slog.Record
}

// Add holds a record until it is flushed.
func (r *bufferedRecords) Add(h slog.Handler, rec slog.Record) {
	r.m.Lock()
	defer r.m.Unlock()

	r.records = append(
		r.records,
		bufferedRecord{
			Handler: h,
			Record:  rec,
		},
	)
}

// Flush writes the held records, and holds no more of them.
func (r *bufferedRecords) Flush(ctx context.Context) {
	for _, rec := range r.take() {
		if rec.Handler.Enabled(ctx, rec.Record.Level) {
			_ = rec.Handler.Handle(ctx, rec.Record)
		}
	}
}

// take returns the held records, and holds no more of them.
func (r *bufferedRecords) take() []bufferedRecord {
	r.m.Lock()
	defer r.m.Unlock()

	records := r.records
	r.records = nil

	return records
}
