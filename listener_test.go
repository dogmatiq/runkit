package runkit

import (
	"context"
	"net"
	"testing"
)

func TestStubListener(t *testing.T) {
	t.Run("it returns the bound address", func(t *testing.T) {
		s := &stubListener{listenAddr: "127.0.0.1:0"}

		addr, err := s.Listen()
		if err != nil {
			t.Fatalf("Listen returned unexpected error: %v", err)
		}
		defer s.listener.Close()

		tcp, ok := addr.(*net.TCPAddr)
		if !ok {
			t.Fatalf("expected *net.TCPAddr, got %T", addr)
		}
		if tcp.Port == 0 || tcp.IP == nil {
			t.Fatalf("expected a bound address, got %v", tcp)
		}
	})

	t.Run("it stops serving when the context is cancelled", func(t *testing.T) {
		s := &stubListener{listenAddr: "127.0.0.1:0"}

		if _, err := s.Listen(); err != nil {
			t.Fatalf("Listen returned unexpected error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.Serve(ctx) }()

		cancel()

		if err := <-done; err != context.Canceled {
			t.Fatalf("Serve returned unexpected error after cancel: %v", err)
		}
	})
}

func TestResolveAdvertiseAddrs(t *testing.T) {
	t.Run("it uses the configured advertise address", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to bind: %v", err)
		}
		defer ln.Close()

		addrs, err := resolveAdvertiseAddrs(ln.Addr(), "127.0.0.1:0", "10.0.0.1:9000")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(addrs) != 1 || addrs[0] != "10.0.0.1:9000" {
			t.Fatalf("got advertise addresses %v, want [%q]", addrs, "10.0.0.1:9000")
		}
	})

	t.Run("it discovers IPv4 addresses when bound to 0.0.0.0", func(t *testing.T) {
		ln, err := net.Listen("tcp4", "0.0.0.0:0")
		if err != nil {
			t.Fatalf("failed to bind: %v", err)
		}
		defer ln.Close()

		addrs, err := resolveAdvertiseAddrs(ln.Addr(), "0.0.0.0:0", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(addrs) == 0 {
			t.Fatal("expected at least one address")
		}

		for _, addr := range addrs {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("invalid address %q: %v", addr, err)
			}
			if host == "" || host == "0.0.0.0" || host == "::" {
				t.Fatalf("expected a non-unspecified host, got %q", host)
			}
			if net.ParseIP(host).To4() == nil {
				t.Fatalf("expected an IPv4 address, got %q", host)
			}
		}
	})

	t.Run("it discovers both families when bound to all interfaces", func(t *testing.T) {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("failed to bind: %v", err)
		}
		defer ln.Close()

		addrs, err := resolveAdvertiseAddrs(ln.Addr(), ":0", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(addrs) == 0 {
			t.Fatal("expected at least one address")
		}

		for _, addr := range addrs {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				t.Fatalf("invalid address %q: %v", addr, err)
			}
			if host == "" || host == "0.0.0.0" || host == "::" {
				t.Fatalf("expected a non-unspecified host, got %q", host)
			}
		}
	})

	t.Run("it uses an explicit listen IP as the advertise address", func(t *testing.T) {
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to bind: %v", err)
		}
		defer ln.Close()

		addr := ln.Addr()
		addrs, err := resolveAdvertiseAddrs(addr, "127.0.0.1:0", "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		wantHost := "127.0.0.1"
		if len(addrs) != 1 {
			t.Fatalf("expected exactly one address, got %v", addrs)
		}
		host, port, err := net.SplitHostPort(addrs[0])
		if err != nil {
			t.Fatalf("invalid address %q: %v", addrs[0], err)
		}
		if host != wantHost {
			t.Fatalf("got host %q, want %q", host, wantHost)
		}
		_, wantPort, _ := net.SplitHostPort(addr.String())
		if port != wantPort {
			t.Fatalf("got port %q, want %s", port, wantPort)
		}
	})
}
