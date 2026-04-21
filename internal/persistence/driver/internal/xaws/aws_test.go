package xaws_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/google/go-cmp/cmp"

	. "github.com/dogmatiq/runkit/internal/persistence/driver/internal/xaws"
)

func TestParseParams(t *testing.T) {
	t.Run("when the URL is valid", func(t *testing.T) {
		cases := []struct {
			Name   string
			URL    string
			Expect Params
		}{
			{"no parameters", "example:///path", Params{}},
			{"region parameter", "example:///path?region=us-east-1", Params{Region: "us-east-1"}},
			{"role_arn parameter", "example:///path?role_arn=arn:aws:iam::123456789012:role/MyRole", Params{RoleARN: "arn:aws:iam::123456789012:role/MyRole"}},
			{"host only", "example://endpoint.host/path", Params{Endpoint: "https://endpoint.host"}},
			{"host with insecure", "example://endpoint.host/path?insecure=true", Params{Endpoint: "http://endpoint.host"}},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				u, err := url.Parse(tc.URL)
				if err != nil {
					t.Fatal(err)
				}
				got, err := ParseParams("example", u)
				if err != nil {
					t.Fatal(err)
				}
				if diff := cmp.Diff(tc.Expect, got); diff != "" {
					t.Fatalf("unexpected result (-want +got):\n%s", diff)
				}
			})
		}
	})

	t.Run("when the URL is invalid", func(t *testing.T) {
		cases := []struct{ Name, URL string }{
			{"insecure without host", "example:///path?insecure=true"},
			{"unknown parameter", "example:///path?unknown=x"},
		}

		for _, tc := range cases {
			t.Run(tc.Name, func(t *testing.T) {
				u, err := url.Parse(tc.URL)
				if err != nil {
					t.Fatal(err)
				}
				_, err = ParseParams("example", u)
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
			})
		}
	})
}

func TestLoadConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("when no parameters are set", func(t *testing.T) {
		_, err := LoadConfig(ctx, Params{})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("when the region is set", func(t *testing.T) {
		cfg, err := LoadConfig(ctx, Params{Region: "us-east-1"})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Region != "us-east-1" {
			t.Fatalf("unexpected region: got %q, want %q", cfg.Region, "us-east-1")
		}
	})

	t.Run("when a custom endpoint is set", func(t *testing.T) {
		_, err := LoadConfig(ctx, Params{Endpoint: "http://localhost:8000"})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("when a role ARN is set", func(t *testing.T) {
		_, err := LoadConfig(ctx, Params{RoleARN: "arn:aws:iam::123456789012:role/MyRole"})
		if err != nil {
			t.Fatal(err)
		}
	})
}
