# BetterStack OpenTelemetry Configuration Notes

Findings from getting all three OTel signals (traces, metrics, logs) working
with BetterStack via the Go OTLP HTTP exporters.

## Traces and metrics work out of the box

The stable v1 exporters (`otlptracehttp`, `otlpmetrichttp`) append the correct
OTLP path automatically when given a base endpoint URL via `WithEndpointURL`.

## Logs require an explicit path

The pre-stable `otlploghttp` v0 exporter does **not** append `/v1/logs`
automatically. Passing the base URL alone causes it to POST to `/`, which
BetterStack accepts with a 202 but silently discards.

Fix — pass the full path explicitly:

```go
otlploghttp.WithEndpointURL(endpoint + "/v1/logs")
```

## Gzip compression is required for logs

## SeverityText must match the canonical severity name

BetterStack uses `SeverityText` as the display label but derives the severity
level color/filter from it — not from `SeverityNumber`. If `SeverityText` is
set to an unrecognized value (e.g. `"BURGER"`), the record displays that string
as the level badge but loses the severity level association entirely.

Always set `SeverityText` to the canonical OTel name (`"INFO"`, `"ERROR"`,
etc.), which is what `severity.String()` returns.

Without `otlploghttp.WithCompression(otlploghttp.GzipCompression)`, logs may
not be stored. Add it alongside the explicit path.

## OTel SDK errors are silently dropped for logs

The log SDK does not route export errors through `otel.SetErrorHandler`. Wrap
the exporter to surface failures:

```go
type loggingLogExporter struct{ sdklog.Exporter }

func (e *loggingLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
    if err := e.Exporter.Export(ctx, records); err != nil {
        log.Printf("log export error: %v", err)
        return err
    }
    return nil
}
```

## Diagnosing export issues

Inject a custom `http.RoundTripper` via `otlploghttp.WithHTTPClient` to dump
the raw request and response. The key things to check:

- The request URL path — should be `POST /v1/logs`
- The response body — a protobuf `partialSuccess` field indicates rejected
  records even on a 202

## OTLP log records appear in Live tail

Once the correct `/v1/logs` path is used, OTLP log records appear in Live
tail alongside natively ingested logs. They are also linked to traces by
`trace_id` and `span_id`.
