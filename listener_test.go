package runkit

import (
	"context"
	"net"
	"testing"
)

func TestStubListener_binds_and_returns_advertise_address(t *testing.T) {
	s := &stubListener{
		bindAddr:      "0.0.0.0:0", // wildcard: triggers firstRoutableIPv4 path
		advertiseAddr: "",          // resolved from bind addr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotAddr string
	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe(ctx, func(addr string) {
			gotAddr = addr
			cancel() // signal we have the address; triggers clean shutdown
		})
	}()

	if err := <-done; err != nil {
		t.Fatalf("ListenAndServe returned error: %v", err)
	}

	if gotAddr == "" {
		t.Fatal("onReady was never called")
	}

	// Address should be host:port with a specific host (not 0.0.0.0).
	host, _, err := net.SplitHostPort(gotAddr)
	if err != nil {
		t.Fatalf("invalid address %q: %v", gotAddr, err)
	}
	if host == "" || host == "0.0.0.0" {
		t.Fatalf("expected a non-unspecified host, got %q", host)
	}
}

func TestStubListener_uses_explicit_advertise_address(t *testing.T) {
	s := &stubListener{
		bindAddr:      "127.0.0.1:0",
		advertiseAddr: "10.0.0.1:9000",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var gotAddr string
	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe(ctx, func(addr string) {
			gotAddr = addr
			cancel()
		})
	}()

	if err := <-done; err != nil {
		t.Fatalf("ListenAndServe returned error: %v", err)
	}

	if gotAddr != "10.0.0.1:9000" {
		t.Fatalf("got advertise address %q, want %q", gotAddr, "10.0.0.1:9000")
	}
}

func TestStubListener_closes_on_context_cancel(t *testing.T) {
	s := &stubListener{bindAddr: "127.0.0.1:0"}

	ctx, cancel := context.WithCancel(context.Background())

	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe(ctx, func(string) { close(ready) })
	}()

	<-ready
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("ListenAndServe returned error after cancel: %v", err)
	}
}
