package telemetry

import (
	"context"
	"log/slog"
	"strconv"

	noopmetric "go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"

	"github.com/dogmatiq/spruce"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
)

// TestingT is the subset of [testing.TB] that is used by the test provider.
type TestingT interface {
	Helper()
	Log(...any)
}

// NewTestProvider returns a [Provider] that records logs to t.
func NewTestProvider(t TestingT) *Provider {
	t.Helper()

	return &Provider{
		TracerProvider: nooptrace.NewTracerProvider(),
		MeterProvider:  noopmetric.NewMeterProvider(),
		LoggerProvider: &testLoggerProvider{
			logger: spruce.NewTestLogger(t),
		},
	}
}

type testLoggerProvider struct {
	embedded.LoggerProvider
	logger *slog.Logger
}

func (p *testLoggerProvider) Logger(name string, _ ...log.LoggerOption) log.Logger {
	return &testLogger{logger: p.logger.WithGroup(name)}
}

type testLogger struct {
	embedded.Logger
	logger *slog.Logger
}

func (l *testLogger) Emit(ctx context.Context, rec log.Record) {
	level := slog.LevelDebug

	switch {
	case rec.Severity() >= log.SeverityError:
		level = slog.LevelError
	case rec.Severity() >= log.SeverityWarn:
		level = slog.LevelWarn
	case rec.Severity() >= log.SeverityInfo:
		level = slog.LevelInfo
	}

	var attrs []slog.Attr

	rec.WalkAttributes(
		func(kv log.KeyValue) bool {
			attrs = append(
				attrs,
				convertValue(kv.Key, kv.Value),
			)
			return true
		},
	)

	if rec.Body().Kind() == log.KindMap {
		for _, pair := range rec.Body().AsMap() {
			attrs = append(
				attrs,
				convertValue(pair.Key, pair.Value),
			)
		}
	} else if !rec.Body().Empty() {
		attrs = append(
			attrs,
			convertValue("body", rec.Body()),
		)
	}

	if !rec.Timestamp().IsZero() {
		attrs = append(attrs, slog.Time("timestamp", rec.Timestamp()))
	}

	if !rec.ObservedTimestamp().IsZero() {
		attrs = append(attrs, slog.Time("observed_timestamp", rec.ObservedTimestamp()))
	}

	l.logger.LogAttrs(
		ctx,
		level,
		rec.EventName(),
		attrs...,
	)
}

func (l *testLogger) Enabled(context.Context, log.EnabledParameters) bool {
	return true
}

func convertValue(name string, v log.Value) slog.Attr {
	switch v.Kind() {
	case log.KindEmpty:
		return slog.Any(name, nil)

	case log.KindBool:
		return slog.Bool(name, v.AsBool())

	case log.KindFloat64:
		return slog.Float64(name, v.AsFloat64())

	case log.KindInt64:
		return slog.Int64(name, v.AsInt64())

	case log.KindString:
		return slog.String(name, v.AsString())

	case log.KindBytes:
		return slog.Any(name, v.AsBytes())

	case log.KindSlice:
		var attrs []slog.Attr
		for i, elem := range v.AsSlice() {
			attrs = append(
				attrs,
				convertValue(
					strconv.Itoa(i),
					elem,
				),
			)
		}
		return slog.GroupAttrs(name, attrs...)

	case log.KindMap:
		var attrs []slog.Attr
		for _, pair := range v.AsMap() {
			attrs = append(
				attrs,
				convertValue(
					pair.Key,
					pair.Value,
				),
			)
		}
		return slog.GroupAttrs(name, attrs...)

	default:
		return slog.String(name, v.String())
	}
}
