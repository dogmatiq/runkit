package heartbeat_test

import (
	"context"
	"testing"
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
		NodeID:        uuidpb.Generate(),
		KVStore:       kvStore,
		AdvertiseAddr: "127.0.0.1:9000",
		Interval:      20 * time.Millisecond,
		GracePeriod:   40 * time.Millisecond,
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
		marshaler.NewProto[*heartbeatpb.HeartbeatRecord, heartbeatpb.HeartbeatRecord](),
	)
	ks, err := store.Open(ctx, "heartbeats")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ks.Close() })
	return ks
}

func waitForRecord(t *testing.T, ks kv.Keyspace[*uuidpb.UUID, *heartbeatpb.HeartbeatRecord], nodeID *uuidpb.UUID) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ok, err := ks.Has(context.Background(), nodeID)
		if err != nil {
			t.Fatalf("checking key existence: %v", err)
		}
		if ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for heartbeat record to be written")
}

func TestWriter_writes_initial_record(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvStore := &memorykv.BinaryStore{}
	w := newTestWriter(t, kvStore)
	ks := openTestKeyspace(t, ctx, kvStore)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx)
	}()

	waitForRecord(t, ks, w.NodeID)

	record, _, err := ks.Get(ctx, w.NodeID)
	if err != nil {
		t.Fatalf("reading record: %v", err)
	}
	if record.Address != w.AdvertiseAddr {
		t.Fatalf("expected Address %q, got %q", w.AdvertiseAddr, record.Address)
	}

	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestWriter_refreshes_record(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvStore := &memorykv.BinaryStore{}
	w := newTestWriter(t, kvStore)
	ks := openTestKeyspace(t, ctx, kvStore)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx)
	}()

	// Run for ~3 intervals to verify multiple refreshes succeed without OCC.
	time.Sleep(3 * w.Interval)

	// Verify the record still exists after multiple refresh intervals.
	ok, err := ks.Has(ctx, w.NodeID)
	if err != nil {
		t.Fatalf("checking record existence: %v", err)
	}
	if !ok {
		t.Fatal("expected record to still exist after refreshes, but it was not found")
	}

	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("expected nil error after refreshes, got: %v", err)
	}
}

func TestWriter_returns_fatal_error_on_OCC_conflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	kvStore := &memorykv.BinaryStore{}
	w := newTestWriter(t, kvStore)

	// Pre-write the node's key so the writer's initial Set(ctx, k, v, 0) conflicts.
	ks := openTestKeyspace(t, ctx, kvStore)
	if err := ks.SetUnconditional(ctx, w.NodeID, &heartbeatpb.HeartbeatRecord{
		Address:   "10.0.0.1:9000",
		ExpiresAt: timestamppb.New(time.Now().Add(time.Minute)),
	}); err != nil {
		t.Fatal(err)
	}

	err := w.Run(ctx)
	if err == nil {
		t.Fatal("expected non-nil error due to OCC conflict, got nil")
	}
}

func TestWriter_graceful_shutdown_deletes_record(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kvStore := &memorykv.BinaryStore{}
	w := newTestWriter(t, kvStore)
	ks := openTestKeyspace(t, context.Background(), kvStore)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx)
	}()

	// Wait for the initial record to be written, then cancel.
	waitForRecord(t, ks, w.NodeID)
	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	// Verify the record was deleted during graceful shutdown.
	ok, err := ks.Has(context.Background(), w.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected record to be deleted after graceful shutdown, but it still exists")
	}
}
