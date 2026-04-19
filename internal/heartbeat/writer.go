package heartbeat

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/runkit/internal/heartbeat/internal/heartbeatpb"
	"github.com/dogmatiq/runkit/internal/x/xpersistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// keyspaceName is the name of the keyspace used to store heartbeat records.
	keyspaceName = "heartbeats"

	// Interval is the time between heartbeat refreshes.
	Interval = 5 * time.Second

	// GracePeriod is added to the heartbeat interval to compute the
	// record's expiry time.
	GracePeriod = 10 * time.Second
)

// Writer periodically writes heartbeat records to a KV store.
type Writer struct {
	// NodeID is the UUID of this node. It is used as the keyspace key.
	NodeID *uuidpb.UUID

	// KVStore is the binary KV store to write heartbeat records into.
	KVStore kv.BinaryStore

	// AdvertiseAddrs are the host:port addresses to write into the record.
	AdvertiseAddrs []string
}

// Run writes an initial heartbeat record and refreshes it on every interval
// tick until ctx is cancelled, at which point the record is deleted.
func (w *Writer) Run(ctx context.Context) error {
	store := kv.NewMarshalingStore(
		w.KVStore,
		xpersistence.UUIDMarshaler,
		marshaler.NewProto[*heartbeatpb.HeartbeatRecord](),
	)

	ks, err := store.Open(ctx, keyspaceName)
	if err != nil {
		return err
	}
	defer ks.Close()

	// rev is the revision of the current heartbeat record. It is updated after
	// each successful write.
	var rev uint64

	for {
		refreshAt := time.Now().Add(Interval)
		expiresAt := refreshAt.Add(GracePeriod)

		err := ks.Set(
			ctx,
			w.NodeID,
			&heartbeatpb.HeartbeatRecord{
				Addresses: w.AdvertiseAddrs,
				ExpiresAt: timestamppb.New(expiresAt),
			},
			rev,
		)

		if err == nil {
			rev++
		} else if kv.IsConflict(err) {
			return fmt.Errorf("heartbeat: OCC conflict on heartbeat write for node %v: %w", w.NodeID, err)
		} else {
			// TODO: log
		}

		select {
		case <-time.After(time.Until(refreshAt)):
			continue
		case <-ctx.Done():
			if rev != 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				if err := ks.Set(ctx, w.NodeID, nil, rev); err != nil {
					return fmt.Errorf("heartbeat: failed to delete heartbeat record for node %v: %w", w.NodeID, err)
				}
			}

			return ctx.Err()
		}
	}
}
