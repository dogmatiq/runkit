package runkit_test

import (
	"net/url"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/persistence/driver/memory"
	"github.com/google/uuid"
)

func TestFromEnvironment(t *testing.T) {
	t.Setenv("DOGMA_SITE_NAME", "test-site")
	t.Setenv("DOGMA_SITE_KEY", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
	t.Setenv("DOGMA_NODE_ID", "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e")
	t.Setenv("DOGMA_PERSISTENCE_URL", "memory:///test-silo")
	t.Setenv("DOGMA_LISTEN_ADDRESS", "0.0.0.0:8000")
	t.Setenv("DOGMA_ADVERTISE_ADDRESS", "10.0.0.1:8000")

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

	t.Run("it reads the persistence URL from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithPersistence wins over environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithPersistenceProvider wins over environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("it reads the listen address from the environment", func(t *testing.T) {
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

func newProvider(t *testing.T) PersistenceProvider {
	t.Helper()
	p, err := memory.NewProvider(&url.URL{
		Scheme: "memory",
		Path:   "/" + uuid.New().String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWithSite(t *testing.T) {
	t.Run("it panics if the name is empty", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithSite("", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
	})

	t.Run("it panics if the key is not a valid UUID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithSite("my-site", "not-a-uuid")
	})
}

func TestWithNodeID(t *testing.T) {
	t.Run("it panics if the ID is not a valid UUID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithNodeID("not-a-uuid")
	})
}

func TestWithPersistence(t *testing.T) {
	t.Run("it panics if the URL is malformed", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithPersistence("memory://%ZZ/silo")
	})

	t.Run("it panics if the URL scheme is unrecognized", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithPersistence("unknown:///silo")
	})
}

func TestWithPersistenceProvider(t *testing.T) {
	t.Run("it panics if the provider is nil", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithPersistenceProvider(nil)
	})
}

func TestWithApplication(t *testing.T) {
	t.Run("it panics if the application is nil", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithApplication(nil)
	})

	t.Run("it panics if an application with the same identity key is already registered", func(t *testing.T) {
		app1 := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("app1", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
			},
		}

		app2 := &stubs.ApplicationStub{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("app2", "c7e6f5d4-b3a2-4918-8f0e-1d2c3b4a5960")
			},
		}

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		New(
			WithApplication(app1),
			WithApplication(app2),
		)
	})
}

func TestWithListenAddress(t *testing.T) {
	t.Run("it sets the listen address", func(t *testing.T) {
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
