package runkit_test

import (
	"testing"

	. "github.com/dogmatiq/runkit"
)

func TestFromEnvironment(t *testing.T) {
	wantSiteName := "test-site"
	wantSite := "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
	wantNode := "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"
	wantListenAddr := "0.0.0.0:8000"
	wantAdvertiseAddr := "10.0.0.1:8000"

	t.Setenv("DOGMA_SITE_NAME", wantSiteName)
	t.Setenv("DOGMA_SITE_KEY", wantSite)
	t.Setenv("DOGMA_NODE_ID", wantNode)
	t.Setenv("DOGMA_LISTEN_ADDRESS", wantListenAddr)
	t.Setenv("DOGMA_ADVERTISE_ADDRESS", wantAdvertiseAddr)

	t.Run("it sets the site identity from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("it sets the node ID from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithSite wins over environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithNodeID wins over environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("it reads the bind address from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("it reads the advertise address from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithListenAddress wins over environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithAdvertiseAddress wins over environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})
}

func TestWithBindAddress(t *testing.T) {
	t.Run("it sets the bind address", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("it panics if the address is empty", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithListenAddress("")
	})
}

func TestWithAdvertiseAddress(t *testing.T) {
	t.Run("it sets the advertise address", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
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
