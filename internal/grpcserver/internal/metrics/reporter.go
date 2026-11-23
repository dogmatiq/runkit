package metrics

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/runkit/internal/dutycycle"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Reporter repeatedly measures a list of [Group] values, and keeps the latest
// sample of each metric in them.
//
// It measures only while at least one caller is using the results. A caller
// starts using them by calling [Reporter.Acquire], and stops by calling the
// function that Acquire returns.
//
// The reporter measures each group on its own timer. [Reporter.MinInterval] and
// [Reporter.MaxDutyCycle] say how often it measures a group.
type Reporter struct {
	// DB is the database to measure.
	DB *sql.DB

	// EngineLogger is the target for log messages produced by the reporter.
	EngineLogger *slog.Logger

	// MinInterval is the shortest time between the starts of two measurements
	// of the same group. It must be positive.
	MinInterval time.Duration

	// MaxDutyCycle is the largest fraction of the time during which the
	// reporter may be measuring a group, counted from the start of one
	// measurement to the start of the next. It must be above 0 and at most 1.
	//
	// For example, if it is 0.1, the next measurement starts at least 20
	// seconds after the start of a measurement that took 2 seconds.
	MaxDutyCycle float64

	// Groups lists the [Group] values to measure. No two groups may have the
	// same metric.
	Groups []Group

	// manifestOnce builds the manifest field the first time that it is needed.
	manifestOnce sync.Once

	// manifest is the manifest of the metrics of the groups.
	manifest Manifest

	// m guards the watchers, stop and done fields.
	m sync.Mutex

	// watchers is the number of callers that hold the result.
	watchers int

	// stop stops the goroutine that measures the groups, which runs while
	// watchers is above zero.
	stop context.CancelFunc

	// done is closed when the last goroutine that measured the groups has
	// returned. It is nil until the first such goroutine starts.
	done <-chan struct{}

	// latest is the result of the measuring so far. Only the goroutine that
	// measures the groups replaces it, and measuring begins by emptying it.
	latest cosyne.Observable[Result]
}

// Result is what a [Reporter] has produced so far: the [Snapshot] of the latest
// samples, or the error that ended the measuring. Once there is an error, the
// samples are final.
type Result struct {
	Snapshot Snapshot
	Err      error
}

// Names returns the full names of the metrics' messages in the reporter's
// [Manifest], in the order of the manifest.
//
// It panics if two groups have the same metric.
func (r *Reporter) Names() []string {
	return r.getManifest().Names()
}

// Acquire returns the latest result of measuring, and a function that releases
// it. The caller must call the function when it has finished with the result.
//
// It starts measuring when the first caller acquires the result. Measuring
// stops when every caller that acquired the result has released it.
//
// It panics unless MinInterval is positive and MaxDutyCycle is above 0 and at
// most 1. It also panics if two groups have the same metric.
func (r *Reporter) Acquire() (cosyne.ObservableR[Result], func()) {
	if r.MinInterval <= 0 {
		panic("metrics minimum interval must be positive")
	}

	if r.MaxDutyCycle <= 0 || r.MaxDutyCycle > 1 {
		panic("metrics maximum duty cycle must be above 0 and at most 1")
	}

	r.getManifest()

	for {
		finishing := r.addWatcher()
		if finishing == nil {
			return &r.latest, r.removeWatcher
		}

		<-finishing
	}
}

// addWatcher counts a new caller of [Reporter.Acquire], and starts measuring
// when it counts the first caller.
//
// If the goroutine of an earlier measuring is still finishing, addWatcher
// returns a channel that is closed when that goroutine returns, and the caller
// waits for the channel and tries again. Starting empties the snapshot, so the
// goroutine has to finish first, or it could store its last measurement in the
// new snapshot. In every other case, addWatcher returns nil.
func (r *Reporter) addWatcher() <-chan struct{} {
	r.m.Lock()
	defer r.m.Unlock()

	if r.watchers == 0 {
		if r.done != nil {
			select {
			case <-r.done:
			default:
				return r.done
			}
		}

		ctx, stop := context.WithCancel(context.Background())
		done := make(chan struct{})

		r.stop = stop
		r.done = done
		r.latest.Store(Result{})

		go func() {
			defer close(done)
			r.measure(ctx)
		}()
	}

	r.watchers++

	return nil
}

// removeWatcher counts the release of a result by a caller of
// [Reporter.Acquire], and stops measuring when it counts the last release.
func (r *Reporter) removeWatcher() {
	r.m.Lock()
	defer r.m.Unlock()

	r.watchers--

	if r.watchers == 0 {
		r.stop()
	}
}

// getManifest returns the manifest of the metrics of the groups.
func (r *Reporter) getManifest() Manifest {
	r.manifestOnce.Do(func() {
		var metrics []ManifestOption

		for _, g := range r.Groups {
			metrics = append(metrics, g.metrics...)
		}

		r.manifest = NewManifest(metrics...)
	})

	return r.manifest
}

// measurement is what measuring one [Group] once has produced.
type measurement struct {
	// Group is the group that was measured.
	Group Group

	// Samples is the measurement of every metric in the group. It is empty if
	// Err is set.
	Samples []*metricsgrpc.WatchMetricsResponse_Sample

	// Err is the error that ended the measuring of the group, or nil if the
	// measurement succeeded.
	Err error
}

// measure measures every [Group] of the reporter, each on its own timer, until
// ctx is canceled or a measurement fails. See [dutycycle.Run] for when each
// measurement starts.
//
// Each time that a measurement changes a sample, it stores a new snapshot in
// r.latest. If a measurement fails, it logs the error, stores it there, and
// stops measuring every group.
func (r *Reporter) measure(ctx context.Context) {
	ctx, stop := context.WithCancel(ctx)
	measurements := make(chan measurement)

	var groups sync.WaitGroup

	for _, group := range r.Groups {
		groups.Go(func() {
			r.measureRepeatedly(ctx, group, measurements)
		})
	}

	r.collect(ctx, measurements)

	stop()
	groups.Wait()
}

// measureRepeatedly repeatedly measures the given group, and sends each
// measurement to the given channel. It returns when ctx is canceled, or after
// it has sent a measurement that failed.
func (r *Reporter) measureRepeatedly(
	ctx context.Context,
	group Group,
	measurements chan<- measurement,
) {
	dutycycle.Run(
		ctx,
		r.MinInterval,
		r.MaxDutyCycle,
		func(ctx context.Context) error {
			m := measurement{Group: group}
			m.Samples, m.Err = r.measureOnce(ctx, group)

			select {
			case measurements <- m:
				return m.Err
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)
}

// measureOnce measures every metric in the given group.
//
// It runs the group's queries in one transaction, with parallel workers off.
// The duty cycle is computed from the elapsed time of the measurement, and with
// parallel workers off, a query's elapsed time is at least its processor time.
func (r *Reporter) measureOnce(
	ctx context.Context,
	group Group,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	var samples []*metricsgrpc.WatchMetricsResponse_Sample

	err := xsql.Transact(
		ctx,
		r.DB,
		func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(
				ctx,
				`SET LOCAL max_parallel_workers_per_gather = 0`,
			); err != nil {
				return fmt.Errorf("unable to turn off parallel workers: %w", err)
			}

			var err error
			samples, err = group.measure(ctx, tx, r.getManifest())
			return err
		},
	)

	return samples, err
}

// collect receives measurements from the groups, and stores a new snapshot in
// r.latest each time that a measurement changes a sample. It returns when ctx
// is canceled or a measurement has failed.
//
// All of the changes of one measurement have the same revision, which is the
// time at which it received the measurement, as a fixed-width, big-endian
// integer. The revision is greater than that of every earlier measurement of
// any group, even if the clock goes backward.
func (r *Reporter) collect(
	ctx context.Context,
	measurements <-chan measurement,
) {
	// revision is the revision of the latest measurement that changed a sample.
	var revision uint64

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-measurements:
			measuredAt := time.Now()

			// The last caller released the result, which stopped the measuring.
			// A measurement that failed because of that is a result of the stop.
			if ctx.Err() != nil {
				return
			}

			if m.Err != nil {
				r.EngineLogger.ErrorContext(
					ctx,
					"unable to measure the engine's metrics",
					slog.String("metric_group", m.Group.name),
					xslog.Error(m.Err),
				)

				r.latest.Store(Result{Err: m.Err})

				return
			}

			latest, _ := r.latest.Load()
			next := latest.Snapshot.With(m.Samples)

			if changes := next.ChangesSince(latest.Snapshot); len(changes) > 0 {
				revision = max(revision+1, uint64(measuredAt.UnixNano()))
				encoded := binary.BigEndian.AppendUint64(nil, revision)

				// The new snapshot is shared when it is stored, so until then
				// the changes can be given their revision.
				for _, c := range changes {
					c.SetRevision(encoded)
				}

				r.latest.Store(Result{Snapshot: next})
			}
		}
	}
}
