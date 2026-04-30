package runkit_test

import (
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/persistencekit"
	"github.com/dogmatiq/persistencekit/driver/memory"
	. "github.com/dogmatiq/runkit"
	"github.com/google/uuid"
)

func TestEnvironmentVariables(t *testing.T) {
	// NOTE: Ferrite caches environment variable values on first read via
	// sync.Once. Since test execution order is non-deterministic, t.Setenv
	// cannot reliably affect Ferrite variables. These tests set env vars
	// before any New() call, but other tests in the package may trigger
	// resolution first.

	t.Setenv("DOGMA_SITE_NAME", "test-site")
	t.Setenv("DOGMA_SITE_KEY", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
	t.Setenv("DOGMA_NODE_ID", "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e")
	t.Setenv("DOGMA_PERSISTENCE_URL", "memory:///test-silo")
	t.Setenv("DOGMA_LISTEN_ADDRESS", "0.0.0.0:8000")
	t.Setenv("DOGMA_ADVERTISE_ADDRESS", "10.0.0.1:8000")

	t.Run("it reads configuration from environment variables by default", func(t *testing.T) {
		t.Skip("TODO: Ferrite caches env vars via sync.Once; test requires process-level env setup")
	})

	t.Run("it sets the site identity from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("it sets the node ID from the environment", func(t *testing.T) {
		t.Skip("TODO: no public API to introspect engine configuration")
	})

	t.Run("explicit WithSiteIdentity wins over environment", func(t *testing.T) {
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

	t.Run("explicit WithPersistenceDriver wins over environment", func(t *testing.T) {
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

// REVIEW: don't need this any longer
func newProvider(t *testing.T) persistencekit.Driver {
	t.Helper()
	return memory.New(uuid.New().String())
}

func TestWithSite(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "e7a24d81-5f36-4c09-8b1e-d4f2a963c750")
		},
	}

	t.Run("it does not cause New() to return an error", func(t *testing.T) {
		if _, err := New(
			app,
			WithSiteIdentity("my-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it panics if the name is empty", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithSiteIdentity("", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d")
	})

	t.Run("it panics if the key is not a valid UUID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithSiteIdentity("my-site", "not-a-uuid")
	})
}

func TestWithNodeID(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "a3b8f192-7c45-4d6e-8f0a-1b2c3d4e5f60")
		},
	}

	t.Run("it does not cause New() to return an error", func(t *testing.T) {
		if _, err := New(
			app,
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
			WithNodeID("b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e"),
		); err != nil {
			t.Fatal(err)
		}
	})

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
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "f9d61e27-8a53-4b0c-9d4f-2e7a38b15c94")
		},
	}

	t.Run("it does not cause New() to return an error", func(t *testing.T) {
		if _, err := New(
			app,
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistence("memory:///test-silo"),
		); err != nil {
			t.Fatal(err)
		}
	})

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

func TestWithPersistenceDriver(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "b4c72f08-6d19-4e5a-8a3b-0f1e2d3c4b5a")
		},
	}

	t.Run("it does not cause New() to return an error", func(t *testing.T) {
		if _, err := New(
			app,
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
		); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("it panics if the provider is nil", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic, got none")
			}
		}()

		WithPersistenceDriver(nil)
	})
}

func TestWithListenAddress(t *testing.T) {
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "d8e95a31-4b67-4c0f-9e2d-7f8a1b3c5d6e")
		},
	}

	t.Run("it does not cause New() to return an error", func(t *testing.T) {
		if _, err := New(
			app,
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
			WithListenAddress("0.0.0.0:8000"),
		); err != nil {
			t.Fatal(err)
		}
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
	app := &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("app", "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d")
		},
	}

	t.Run("it does not cause New() to return an error", func(t *testing.T) {
		if _, err := New(
			app,
			WithSiteIdentity("test-site", "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"),
			WithPersistenceDriver(newProvider(t)),
			WithListenAddress("0.0.0.0:8000"),
			WithAdvertiseAddress("10.0.0.1:8000"),
		); err != nil {
			t.Fatal(err)
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
