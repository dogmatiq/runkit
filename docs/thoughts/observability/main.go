// otelgen generates OpenTelemetry signals that approximate what a real Runkit
// engine might emit. It uses in-memory persistencekit primitives (with their
// built-in OTel instrumentation) and adds a fake three-service call chain on
// top: api-gateway -> order-service -> payment-service.
//
// Build and run from this directory:
//
//	go run .
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	stdlog "log"
	"net/http"
	"net/http/httputil"
	"os"
	"os/signal"
	"time"

	"github.com/dogmatiq/persistencekit/driver/memory/memoryjournal"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/journal"
	"github.com/dogmatiq/persistencekit/kv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	betterStackEndpoint = "https://s2368482.eu-fsn-3.betterstackdata.com"
	betterStackToken    = "mmhZbBszv7jxUTiTfzVZpRhA"
)

// services holds a tracer per simulated service plus a shared meter and logger.
type services struct {
	api     trace.Tracer
	order   trace.Tracer
	payment trace.Tracer
	meter   metric.Meter
	logger  otellog.Logger
}

func main() {
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		stdlog.Printf("otel export error: %v", err)
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	svc, j, ks, shutdown, err := setup(ctx)
	if err != nil {
		stdlog.Fatalf("setup: %v", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := shutdown(shutCtx); err != nil {
			stdlog.Printf("shutdown: %v", err)
		}
	}()

	ordersProcessed, err := svc.meter.Int64Counter(
		"orders.processed",
		metric.WithDescription("Number of orders processed successfully"),
	)
	if err != nil {
		stdlog.Fatalf("create counter: %v", err)
	}
	ordersFailed, err := svc.meter.Int64Counter(
		"orders.failed",
		metric.WithDescription("Number of orders that failed"),
	)
	if err != nil {
		stdlog.Fatalf("create counter: %v", err)
	}

	stdlog.Println("running — press ctrl-c to stop")

	for i := 1; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if traceID, err := runScenario(ctx, svc, j, ks, i); err != nil {
			stdlog.Printf("order %d [%s]: error: %v", i, traceID, err)
			ordersFailed.Add(ctx, 1)
		} else {
			stdlog.Printf("order %d [%s]: ok", i, traceID)
			ordersProcessed.Add(ctx, 1)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func setup(ctx context.Context) (svc services, j journal.BinaryJournal, ks kv.BinaryKeyspace, shutdown func(context.Context) error, err error) {
	headers := map[string]string{
		"Authorization": "Bearer " + betterStackToken,
	}

	traceExp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(betterStackEndpoint),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
	)
	if err != nil {
		return svc, nil, nil, nil, fmt.Errorf("trace exporter: %w", err)
	}

	metricExp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(betterStackEndpoint),
		otlpmetrichttp.WithHeaders(headers),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
	)
	if err != nil {
		return svc, nil, nil, nil, fmt.Errorf("metric exporter: %w", err)
	}

	logExp, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(betterStackEndpoint+"/v1/logs"),
		otlploghttp.WithHeaders(headers),
		otlploghttp.WithCompression(otlploghttp.GzipCompression),
		otlploghttp.WithHTTPClient(&http.Client{Transport: &loggingTransport{}}),
	)
	if err != nil {
		return svc, nil, nil, nil, fmt.Errorf("log exporter: %w", err)
	}

	// Each service gets its own TracerProvider with a distinct service.name
	// resource, but all share the same underlying exporter. Spans from all
	// three services appear in BetterStack as separate services within the
	// same trace.
	newTP := func(serviceName string) *sdktrace.TracerProvider {
		res := resource.NewWithAttributes("",
			attribute.String("service.name", serviceName),
			attribute.String("service.version", "dev"),
		)
		return sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(traceExp),
			sdktrace.WithResource(res),
		)
	}

	apiTP := newTP("api-gateway")
	orderTP := newTP("order-service")
	paymentTP := newTP("payment-service")

	sharedRes := resource.NewWithAttributes("",
		attribute.String("service.name", "otelgen"),
		attribute.String("service.version", "dev"),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
			metricExp,
			sdkmetric.WithInterval(10*time.Second),
		)),
		sdkmetric.WithResource(sharedRes),
	)
	stdoutExp, err := stdoutlog.New()
	if err != nil {
		return svc, nil, nil, nil, fmt.Errorf("stdout log exporter: %w", err)
	}

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(&loggingLogExporter{logExp})),
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(stdoutExp)),
		sdklog.WithResource(sharedRes),
	)

	// Wrap in-memory stores with built-in OTel instrumentation from
	// persistencekit. Use orderTP so persistence spans appear under
	// order-service in the trace.
	jStore := journal.WithTelemetry(&memoryjournal.BinaryStore{}, orderTP, mp, lp)
	kvStore := kv.WithTelemetry(&memorykv.BinaryStore{}, orderTP, mp, lp)

	j, err = jStore.Open(ctx, "orders")
	if err != nil {
		return svc, nil, nil, nil, fmt.Errorf("journal open: %w", err)
	}

	ks, err = kvStore.Open(ctx, "state")
	if err != nil {
		return svc, nil, nil, nil, fmt.Errorf("keyspace open: %w", err)
	}

	svc = services{
		api:     apiTP.Tracer("api-gateway"),
		order:   orderTP.Tracer("order-service"),
		payment: paymentTP.Tracer("payment-service"),
		meter:   mp.Meter("otelgen"),
		logger:  lp.Logger("otelgen"),
	}

	shutdown = func(ctx context.Context) error {
		return errors.Join(
			j.Close(),
			ks.Close(),
			apiTP.Shutdown(ctx),
			orderTP.Shutdown(ctx),
			paymentTP.Shutdown(ctx),
			mp.Shutdown(ctx),
			lp.Shutdown(ctx),
		)
	}

	return svc, j, ks, shutdown, nil
}

// runScenario simulates one order flowing through three services.
//
// Latency pattern (by orderID):
//   - Every 7th:  fast failure in payment-service (payment declined)
//   - Every 11th: very slow payment (2.5s)
//   - Every 5th:  slow payment (700ms)
//   - Default:    fast (10-50ms, varies by order ID)
func runScenario(ctx context.Context, svc services, j journal.BinaryJournal, ks kv.BinaryKeyspace, orderID int) (trace.TraceID, error) {
	customer := fmt.Sprintf("customer-%d", (orderID%5)+1)

	// api-gateway: server span representing an inbound HTTP request.
	ctx, apiSpan := svc.api.Start(ctx, "POST /orders",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("url.path", "/orders"),
			attribute.Int("order.id", orderID),
			attribute.String("order.customer", customer),
		),
	)
	defer apiSpan.End()

	traceID := apiSpan.SpanContext().TraceID()

	if err := handleOrder(ctx, svc, j, ks, orderID, customer); err != nil {
		apiSpan.SetAttributes(
			attribute.Int("http.response.status_code", 500),
			attribute.String("error.type", "internal_error"),
		)
		apiSpan.RecordError(err)
		apiSpan.SetStatus(codes.Error, err.Error())
		return traceID, err
	}

	apiSpan.SetAttributes(attribute.Int("http.response.status_code", 200))
	return traceID, nil
}

// handleOrder runs inside order-service: persists the order then calls
// payment-service. If payment fails, the error propagates back through this
// span and up to the api-gateway span.
func handleOrder(ctx context.Context, svc services, j journal.BinaryJournal, ks kv.BinaryKeyspace, orderID int, customer string) error {
	ctx, span := svc.order.Start(ctx, "order.handle",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int("order.id", orderID),
			attribute.String("order.customer", customer),
		),
	)
	defer span.End()

	// Persist order event to journal.
	// persistencekit emits journal.bounds and journal.append child spans.
	bounds, err := j.Bounds(ctx)
	if err != nil {
		span.SetAttributes(attribute.String("error.type", "journal_bounds"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("journal bounds: %w", err)
	}
	rec := []byte(fmt.Sprintf(
		`{"order_id":%d,"customer":%q,"ts":%d}`,
		orderID, customer, time.Now().UnixNano(),
	))
	if err := j.Append(ctx, bounds.End, rec); err != nil {
		span.SetAttributes(attribute.String("error.type", "journal_append"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("journal append: %w", err)
	}

	// Update per-customer order count in KV store.
	// persistencekit emits keyspace.get and keyspace.set child spans.
	key := []byte(customer)
	v, rev, err := ks.Get(ctx, key)
	if err != nil {
		span.SetAttributes(attribute.String("error.type", "kv_get"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kv get: %w", err)
	}
	var count uint64
	if len(v) == 8 {
		count = binary.BigEndian.Uint64(v)
	}
	count++
	newV := make([]byte, 8)
	binary.BigEndian.PutUint64(newV, count)
	if err := ks.Set(ctx, key, newV, rev); err != nil {
		span.SetAttributes(attribute.String("error.type", "kv_set"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("kv set: %w", err)
	}

	span.SetAttributes(attribute.Int64("customer.order_count", int64(count)))
	emitLog(ctx, svc.logger, otellog.SeverityInfo,
		fmt.Sprintf("order %d persisted for %s (total orders: %d)", orderID, customer, count),
		otellog.Int("order.id", orderID),
		otellog.String("order.customer", customer),
		otellog.Int64("customer.order_count", int64(count)),
	)

	// Call payment-service. If it fails, mark this span as an error too so
	// the failure is visible on every span in the chain.
	if err := chargePayment(ctx, svc, orderID, customer); err != nil {
		span.SetAttributes(attribute.String("error.type", "payment_failed"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// chargePayment runs inside payment-service. It simulates variable processing
// latency and occasional declines.
func chargePayment(ctx context.Context, svc services, orderID int, customer string) error {
	latency := paymentLatency(orderID)

	_, span := svc.payment.Start(ctx, "payment.charge",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int("order.id", orderID),
			attribute.String("payment.customer", customer),
			attribute.Int64("payment.latency_ms", int64(latency/time.Millisecond)),
		),
	)
	defer span.End()

	if latency > 0 {
		select {
		case <-ctx.Done():
			span.SetAttributes(attribute.String("error.type", "context_cancelled"))
			span.RecordError(ctx.Err())
			span.SetStatus(codes.Error, "context cancelled")
			return ctx.Err()
		case <-time.After(latency):
		}
	}

	if orderID%7 == 0 {
		err := fmt.Errorf("payment declined: insufficient funds")
		span.SetAttributes(attribute.String("error.type", "payment_declined"))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		emitLog(ctx, svc.logger, otellog.SeverityError,
			fmt.Sprintf("payment declined for order %d (%s)", orderID, customer),
			otellog.Int("order.id", orderID),
			otellog.String("payment.customer", customer),
		)
		return err
	}

	span.SetAttributes(attribute.String("payment.status", "approved"))
	emitLog(ctx, svc.logger, otellog.SeverityInfo,
		fmt.Sprintf("payment approved for order %d (%s)", orderID, customer),
		otellog.Int("order.id", orderID),
		otellog.String("payment.customer", customer),
	)
	return nil
}

// paymentLatency returns a deterministic simulated payment processing duration.
// Every 7th order fails fast, every 11th is very slow, every 5th is slow,
// and the rest vary between 10ms and 50ms based on order ID.
func paymentLatency(orderID int) time.Duration {
	switch {
	case orderID%7 == 0:
		return 0
	case orderID%11 == 0:
		return 2500 * time.Millisecond
	case orderID%5 == 0:
		return 700 * time.Millisecond
	default:
		return time.Duration(10+orderID%40) * time.Millisecond
	}
}

func emitLog(ctx context.Context, logger otellog.Logger, severity otellog.Severity, body string, attrs ...otellog.KeyValue) {
	now := time.Now()
	var rec otellog.Record
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetSeverity(severity)
	rec.SetSeverityText(severity.String())
	rec.SetBody(otellog.StringValue(body))
	rec.AddAttributes(attrs...)
	logger.Emit(ctx, rec)
}

// loggingLogExporter wraps an sdklog.Exporter and prints any export errors to
// stdout. The log SDK does not route errors through otel.SetErrorHandler, so
// without this wrapper, export failures are silently discarded.
type loggingLogExporter struct {
	sdklog.Exporter
}

func (e *loggingLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	if err := e.Exporter.Export(ctx, records); err != nil {
		stdlog.Printf("log export error: %v", err)
		return err
	}
	return nil
}

// loggingTransport is an http.RoundTripper that dumps every request and
// response made by the log exporter so we can see exactly what BetterStack
// receives and returns.
type loggingTransport struct{}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reqDump, _ := httputil.DumpRequestOut(req, true)
	stdlog.Printf("log HTTP request:\n%s", reqDump)

	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		stdlog.Printf("log HTTP error: %v", err)
		return nil, err
	}

	respDump, _ := httputil.DumpResponse(resp, true)
	stdlog.Printf("log HTTP response:\n%s", respDump)
	return resp, nil
}
