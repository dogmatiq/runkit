// Package metrics measures the engine for the MetricsAPI service of
// dogmatiq/protobuf, and keeps the latest samples for the service to send. It
// uses the terms of that protocol, such as metric, subject, sample and
// revision.
//
// A [Group] is a set of metrics that are measured together. A [Reporter]
// measures groups on timers, and keeps the latest sample of each metric and
// subject in a [Snapshot]. A [Manifest] lists the metrics of one reporter, and
// gives each an index for its samples to carry.
package metrics
