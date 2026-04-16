package heartbeat

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/runkit/internal/heartbeat/internal/heartbeatpb"
	xpersistence "github.com/dogmatiq/runkit/internal/x/xpersistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const keyspaceName = "heartbeats"

// Writer writes heartbeat records to a KV store, periodically refreshing them
// until the context is cancelled.
type Writer struct {
	// NodeID is the UUID of this node. It is used as the keyspace key.
	NodeID *uuidpb.UUID

	// KVStore is the binary KV store to write heartbeat records into.
	KVStore kv.BinaryStore

	// AdvertiseAddr is the host:port address to write into the record.
	AdvertiseAddr string

	// Interval is the time between heartbeat refreshes.
	// Zero means 5 seconds.
	Interval time.Duration

	// GracePeriod is added to Interval to compute the record's expiry time.
	// Zero means 10 seconds.
	GracePeriod time.Duration
}

// Run writes an initial heartbeat record and refreshes it on every interval
// tick until ctx is cancelled, at which point the record is deleted.
func (w *Writer) Run(ctx context.Context) error {
	interval := w.Interval
	if interval == 0 {
		interval = 5 * time.Second
	}

	gracePeriod := w.GracePeriod
	if gracePeriod == 0 {
		gracePeriod = 10 * time.Second
	}

	store := kv.NewMarshalingStore(
		w.KVStore,
		xpersistence.UUIDMarshaler,
		marshaler.NewProto[*heartbeatpb.HeartbeatRecord, heartbeatpb.HeartbeatRecord](),
	)

	ks, err := store.Open(ctx, keyspaceName)
	if err != nil {
		return err
	}
	defer ks.Close()

	newRecord := func(now time.Time) *heartbeatpb.HeartbeatRecord {
		return &heartbeatpb.HeartbeatRecord{
			Address:   w.AdvertiseAddr,
			ExpiresAt: timestamppb.New(now.Add(interval + gracePeriod)),
		}
	}

	gracefulDelete := func() {
		freshCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ks.SetUnconditional(freshCtx, w.NodeID, nil)
	}

	// Write the initial heartbeat record at revision 0 (key must not exist).
	var expiry time.Time
	for {
		now := time.Now()
		err := ks.Set(ctx, w.NodeID, newRecord(now), 0)
		if err == nil {
			expiry = now.Add(interval + gracePeriod)
			break
		}

		if kv.IsConflict(err) {
			return fmt.Errorf("heartbeat: UUID collision detected for node %v - another node is using the same ID: %w", w.NodeID, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}

	rev := uint64(1)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			gracefulDelete()
			return nil

		case <-ticker.C:
			for {
				now := time.Now()
				err := ks.Set(ctx, w.NodeID, newRecord(now), rev)
				if err == nil {
					rev++
					expiry = now.Add(interval + gracePeriod)
					break
				}

				if kv.IsConflict(err) {
					return fmt.Errorf("heartbeat: OCC conflict on heartbeat refresh: %w", err)
				}

				if time.Now().After(expiry) {
					return fmt.Errorf("heartbeat: failed to refresh record before expiry: %w", err)
				}

				select {
				case <-ctx.Done():
					gracefulDelete()
					return nil
				case <-time.After(time.Second):
				}
			}
		}
	}
}
