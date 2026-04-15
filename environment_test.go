package runkit

import (
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
)

func TestFromEnvironment(t *testing.T) {
	wantSiteName := "test-site"
	wantSite := "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
	wantNode := "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"
	wantBindAddr := "0.0.0.0:8000"
	wantAdvertiseAddr := "10.0.0.1:8000"

	t.Setenv("DOGMA_SITE_NAME", wantSiteName)
	t.Setenv("DOGMA_SITE_KEY", wantSite)
	t.Setenv("DOGMA_NODE_ID", wantNode)
	t.Setenv("DOGMA_BIND_ADDRESS", wantBindAddr)
	t.Setenv("DOGMA_ADVERTISE_ADDRESS", wantAdvertiseAddr)

	t.Run("it sets the site identity from the environment", func(t *testing.T) {
		e := New(FromEnvironment())

		want, err := uuidpb.Parse(wantSite)
		if err != nil {
			t.Fatal(err)
		}

		if e.site.Name != wantSiteName {
			t.Fatalf("got site name %q, want %q", e.site.Name, wantSiteName)
		}

		if !e.site.Key.Equal(want) {
			t.Fatalf("got site key %s, want %s", e.site.Key, want)
		}
	})

	t.Run("it sets the node ID from the environment", func(t *testing.T) {
		e := New(
			WithSite("test-site", wantSite),
			FromEnvironment(),
		)

		want, err := uuidpb.Parse(wantNode)
		if err != nil {
			t.Fatal(err)
		}

		if !e.nodeID.Equal(want) {
			t.Fatalf("got node ID %s, want %s", e.nodeID, want)
		}
	})

	t.Run("explicit WithSite wins over environment", func(t *testing.T) {
		explicit := "22222222-2222-4222-8222-222222222222"
		e := New(
			FromEnvironment(),
			WithSite("explicit-site", explicit),
		)

		want, err := uuidpb.Parse(explicit)
		if err != nil {
			t.Fatal(err)
		}

		if !e.site.Key.Equal(want) {
			t.Fatalf("got site key %s, want %s", e.site.Key, want)
		}
	})

	t.Run("explicit WithNodeID wins over environment", func(t *testing.T) {
		explicit := "33333333-3333-4333-8333-333333333333"
		e := New(
			WithSite("test-site", wantSite),
			FromEnvironment(),
			WithNodeID(explicit),
		)

		want, err := uuidpb.Parse(explicit)
		if err != nil {
			t.Fatal(err)
		}

		if !e.nodeID.Equal(want) {
			t.Fatalf("got node ID %s, want %s", e.nodeID, want)
		}
	})

	t.Run("it reads the bind address from the environment", func(t *testing.T) {
		e := New(FromEnvironment())
		if e.bindAddr != wantBindAddr {
			t.Fatalf("got %q, want %q", e.bindAddr, wantBindAddr)
		}
	})

	t.Run("it reads the advertise address from the environment", func(t *testing.T) {
		e := New(FromEnvironment())
		if e.advertiseAddr != wantAdvertiseAddr {
			t.Fatalf("got %q, want %q", e.advertiseAddr, wantAdvertiseAddr)
		}
	})

	t.Run("explicit WithBindAddress wins over environment", func(t *testing.T) {
		e := New(FromEnvironment(), WithBindAddress("127.0.0.1:9000"))
		if e.bindAddr != "127.0.0.1:9000" {
			t.Fatalf("got %q, want %q", e.bindAddr, "127.0.0.1:9000")
		}
	})

	t.Run("explicit WithAdvertiseAddress wins over environment", func(t *testing.T) {
		e := New(FromEnvironment(), WithAdvertiseAddress("192.168.1.1:9000"))
		if e.advertiseAddr != "192.168.1.1:9000" {
			t.Fatalf("got %q, want %q", e.advertiseAddr, "192.168.1.1:9000")
		}
	})
}

func TestWithBindAddress(t *testing.T) {
	t.Run("it sets the bind address", func(t *testing.T) {
		e := New(WithBindAddress("127.0.0.1:9000"))
		if e.bindAddr != "127.0.0.1:9000" {
			t.Fatalf("got %q, want %q", e.bindAddr, "127.0.0.1:9000")
		}
	})

	t.Run("it panics if the address is empty", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithBindAddress("")
	})
}

func TestWithAdvertiseAddress(t *testing.T) {
	t.Run("it sets the advertise address", func(t *testing.T) {
		e := New(WithAdvertiseAddress("192.168.1.1:9000"))
		if e.advertiseAddr != "192.168.1.1:9000" {
			t.Fatalf("got %q, want %q", e.advertiseAddr, "192.168.1.1:9000")
		}
	})

	t.Run("it panics if the address is empty", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithAdvertiseAddress("")
	})
}
