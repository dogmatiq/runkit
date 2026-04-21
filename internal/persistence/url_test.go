package persistence_test

import (
	"bytes"
	"strings"
	"testing"

	. "github.com/dogmatiq/runkit/internal/persistence"
)

func TestProviderFromURL(t *testing.T) {
	t.Run("memory://", func(t *testing.T) {
		t.Run("when the silo name is missing", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("memory://")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})

		t.Run("memory:///silo", func(t *testing.T) {
			t.Run("it returns a valid provider", func(t *testing.T) {
				_, err := ProviderFromURL("memory:///mysilo")
				if err != nil {
					t.Fatal(err)
				}
			})
		})

		t.Run("when the host is non-empty", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("memory://myname")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})

		t.Run("when query parameters are present", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("memory://?foo=bar")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})

		t.Run("when two providers use the same silo", func(t *testing.T) {
			t.Run("it returns providers that share KV state", func(t *testing.T) {
				silo := strings.ReplaceAll(t.Name(), "/", "-")
				rawURL := "memory:///" + silo

				p1, err := ProviderFromURL(rawURL)
				if err != nil {
					t.Fatal(err)
				}
				p2, err := ProviderFromURL(rawURL)
				if err != nil {
					t.Fatal(err)
				}

				store1, err := p1.KVStore(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				ks1, err := store1.Open(t.Context(), "test")
				if err != nil {
					t.Fatal(err)
				}
				defer ks1.Close()

				key := []byte("key")
				want := []byte("value")
				if err := ks1.Set(t.Context(), key, want, 0); err != nil {
					t.Fatal(err)
				}

				store2, err := p2.KVStore(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				ks2, err := store2.Open(t.Context(), "test")
				if err != nil {
					t.Fatal(err)
				}
				defer ks2.Close()

				got, _, err := ks2.Get(t.Context(), key)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("expected shared KV state: got %q, want %q", got, want)
				}
			})
		})
	})

	t.Run("postgres://", func(t *testing.T) {
		t.Run("it returns a valid provider", func(t *testing.T) {
			_, err := ProviderFromURL("postgres://user:pass@host/db")
			if err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("postgresql://", func(t *testing.T) {
		t.Run("it returns a valid provider", func(t *testing.T) {
			_, err := ProviderFromURL("postgresql://user:pass@host/db")
			if err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("dynamodb://", func(t *testing.T) {
		t.Run("it returns a valid provider", func(t *testing.T) {
			_, err := ProviderFromURL("dynamodb:///myapp")
			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("when a valid region parameter is present", func(t *testing.T) {
			t.Run("it returns a valid provider", func(t *testing.T) {
				_, err := ProviderFromURL("dynamodb:///myapp?region=us-east-1")
				if err != nil {
					t.Fatal(err)
				}
			})
		})

		t.Run("when an unknown parameter is present", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("dynamodb:///myapp?unknown=x")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})

		t.Run("when the base name is missing", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("dynamodb://")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})
	})

	t.Run("s3://", func(t *testing.T) {
		t.Run("it returns a valid provider", func(t *testing.T) {
			_, err := ProviderFromURL("s3:///my-bucket")
			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("when a custom endpoint is provided", func(t *testing.T) {
			t.Run("it returns a valid provider", func(t *testing.T) {
				_, err := ProviderFromURL("s3://endpoint.host/my-bucket")
				if err != nil {
					t.Fatal(err)
				}
			})
		})

		t.Run("when the insecure parameter is present", func(t *testing.T) {
			t.Run("it returns a valid provider", func(t *testing.T) {
				_, err := ProviderFromURL("s3:///my-bucket?insecure=true")
				if err != nil {
					t.Fatal(err)
				}
			})
		})

		t.Run("when an unknown parameter is present", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("s3:///my-bucket?unknown=x")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})

		t.Run("when the bucket name is missing", func(t *testing.T) {
			t.Run("it returns an error", func(t *testing.T) {
				_, err := ProviderFromURL("s3://")
				if err == nil {
					t.Fatal("expected an error")
				}
			})
		})
	})

	t.Run("when the scheme is unknown", func(t *testing.T) {
		t.Run("it returns an error", func(t *testing.T) {
			_, err := ProviderFromURL("unknown://foo")
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	})

	t.Run("when the URL has no scheme", func(t *testing.T) {
		t.Run("it returns an error", func(t *testing.T) {
			_, err := ProviderFromURL("no-double-slash")
			if err == nil {
				t.Fatal("expected an error")
			}
		})
	})
}
