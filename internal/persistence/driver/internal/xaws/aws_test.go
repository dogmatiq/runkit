package xaws_test

import (
	"context"
	"net/url"
	"testing"

	. "github.com/dogmatiq/runkit/internal/persistence/driver/internal/xaws"
)

func TestParseConfig(t *testing.T) {
	t.Run("when the host is set", func(t *testing.T) {
		u, _ := url.Parse("example://endpoint.host/path")
		load, err := ParseConfig(u)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.BaseEndpoint == nil || *cfg.BaseEndpoint != "https://endpoint.host" {
			t.Fatalf("unexpected endpoint: got %v, want %q", cfg.BaseEndpoint, "https://endpoint.host")
		}
	})

	t.Run("when the insecure parameter is set", func(t *testing.T) {
		u, _ := url.Parse("example://endpoint.host/path?insecure")
		load, err := ParseConfig(u)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.BaseEndpoint == nil || *cfg.BaseEndpoint != "http://endpoint.host" {
			t.Fatalf("unexpected endpoint: got %v, want %q", cfg.BaseEndpoint, "http://endpoint.host")
		}
	})

	t.Run("when the region parameter is set", func(t *testing.T) {
		u, _ := url.Parse("example:///path?region=us-east-1")
		load, err := ParseConfig(u)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := load(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Region != "us-east-1" {
			t.Fatalf("unexpected region: got %q, want %q", cfg.Region, "us-east-1")
		}
	})

	t.Run("when the role_arn parameter is set", func(t *testing.T) {
		u, _ := url.Parse("example:///path?role_arn=arn:aws:iam::123456789012:role/MyRole")
		if _, err := ParseConfig(u); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"insecure without host", "example:///path?insecure"},
			{"unknown parameter", "example:///path?unknown=x"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				u, err := url.Parse(tc.URL)
				if err != nil {
					t.Fatal(err)
				}
				_, err = ParseConfig(u)
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
			})
		}
	})
}
