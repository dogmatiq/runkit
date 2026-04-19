package heartbeat_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/driver/memory/memorykv"
	"github.com/dogmatiq/persistencekit/kv"
	"github.com/dogmatiq/persistencekit/marshaler"
	"github.com/dogmatiq/runkit/internal/heartbeat"
	"github.com/dogmatiq/runkit/internal/heartbeat/internal/heartbeatpb"
	xpersistence "github.com/dogmatiq/runkit/internal/x/xpersistence"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestWriter(t *testing.T, kvStore kv.BinaryStore) *heartbeat.Writer {
	t.Helper()
	return &heartbeat.Writer{
		NodeID:         uuidpb.Generate(),
		KVStore:        kvStore,
		AdvertiseAddrs: []string{"127.0.0.1:9000", "[::1]:9000"},
	}
}

func openTestKeyspace(
	t *testing.T,
	ctx context.Context,
	kvStore kv.BinaryStore,
) kv.Keyspace[*uuidpb.UUID, *heartbeatpb.HeartbeatRecord] {
	t.Helper()
	store := kv.NewMarshalingStore(
		kvStore,
		xpersistence.UUIDMarshaler,
		marshaler.NewProto[*heartbeatpb.HeartbeatRecord](),
	)
	ks, err := store.Open(ctx, "heartbeats")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ks.Close() })
	return ks
}

func TestWriter(t *testing.T) {
	t.Run("it writes the initial heartbeat record", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kvStore := &memorykv.BinaryStore{}
			w := newTestWriter(t, kvStore)
			ks := openTestKeyspace(t, ctx, kvStore)

			errCh := make(chan error, 1)
			go func() {
				errCh <- w.Run(ctx)
			}()

			// Wait for the goroutine to block (record written, now waiting on ticker).
			synctest.Wait()

			record, _, err := ks.Get(ctx, w.NodeID)
			if err != nil {
				t.Fatalf("reading record: %v", err)
			}
			if len(record.Addresses) != len(w.AdvertiseAddrs) {
				t.Fatalf("expected Addresses %v, got %v", w.AdvertiseAddrs, record.Addresses)
			}
			for i, want := range w.AdvertiseAddrs {
				if record.Addresses[i] != want {
					t.Fatalf("expected Addresses[%d] %q, got %q", i, want, record.Addresses[i])
				}
			}

			cancel()
			synctest.Wait()

			if err := <-errCh; !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got: %v", err)
			}
		})
	})

	t.Run("it refreshes the record on each interval", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kvStore := &memorykv.BinaryStore{}
			w := newTestWriter(t, kvStore)
			ks := openTestKeyspace(t, ctx, kvStore)

			errCh := make(chan error, 1)
			go func() {
				errCh <- w.Run(ctx)
			}()

			// Advance through 3 intervals to verify multiple refreshes succeed without OCC.
			for range 3 {
				synctest.Wait()
				time.Sleep(heartbeat.Interval)
				synctest.Wait()
			}

			// Verify the record still exists after multiple refresh intervals.
			ok, err := ks.Has(ctx, w.NodeID)
			if err != nil {
				t.Fatalf("checking record existence: %v", err)
			}
			if !ok {
				t.Fatal("expected record to still exist after refreshes, but it was not found")
			}

			cancel()
			synctest.Wait()

			if err := <-errCh; !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got: %v", err)
			}
		})
	})

	t.Run("it returns a fatal error when the initial write has an OCC conflict", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			kvStore := &memorykv.BinaryStore{}
			w := newTestWriter(t, kvStore)

			// Pre-write the node's key so the writer's initial Set(ctx, k, v, 0) conflicts.
			ks := openTestKeyspace(t, ctx, kvStore)
			if err := ks.SetUnconditional(ctx, w.NodeID, &heartbeatpb.HeartbeatRecord{
				Addresses: []string{"10.0.0.1:9000"},
				ExpiresAt: timestamppb.New(time.Now().Add(time.Minute)),
			}); err != nil {
				t.Fatal(err)
			}

			errCh := make(chan error, 1)
			go func() {
				errCh <- w.Run(ctx)
			}()

			synctest.Wait()

			err := <-errCh
			if err == nil {
				t.Fatal("expected non-nil error due to OCC conflict, got nil")
			}
		})
	})

	t.Run("it deletes the record on graceful shutdown", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kvStore := &memorykv.BinaryStore{}
			w := newTestWriter(t, kvStore)
			ks := openTestKeyspace(t, context.Background(), kvStore)

			errCh := make(chan error, 1)
			go func() {
				errCh <- w.Run(ctx)
			}()

			// Wait for initial record to be written, then cancel.
			synctest.Wait()
			cancel()
			synctest.Wait()

			if err := <-errCh; !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context.Canceled, got: %v", err)
			}

			ok, err := ks.Has(context.Background(), w.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatal("expected record to be deleted after graceful shutdown, but it still exists")
			}
		})
	})
}
