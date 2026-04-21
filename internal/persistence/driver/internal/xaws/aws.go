// Package xaws provides shared AWS helpers for persistence drivers.
package xaws

import (
	"context"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Params holds the AWS connection parameters common to all AWS-backed
// persistence drivers.
type Params struct {
	Region   string
	RoleARN  string
	Endpoint string
}

// ParseParams extracts the common AWS query parameters from u. scheme is the
// URL scheme expected by the calling driver, used only in error messages. The
// caller is responsible for validating u.Scheme before calling this function.
//
// The endpoint is constructed from u.Host and the "insecure" query parameter:
// if u.Host is non-empty the endpoint is "https://<host>" by default, or
// "http://<host>" when insecure=true.
func ParseParams(scheme string, u *url.URL) (Params, error) {
	q := u.Query()
	var p Params

	p.Region = q.Get("region")
	q.Del("region")

	p.RoleARN = q.Get("role_arn")
	q.Del("role_arn")

	insecure := q.Get("insecure") == "true"
	q.Del("insecure")

	if u.Host != "" {
		proto := "https"
		if insecure {
			proto = "http"
		}
		p.Endpoint = proto + "://" + u.Host
	} else if insecure {
		return Params{}, fmt.Errorf("invalid %s URL: insecure=true has no effect without a host", scheme)
	}

	for k := range q {
		return Params{}, fmt.Errorf("invalid %s URL: unknown parameter %q", scheme, k)
	}

	return p, nil
}

// LoadConfig loads an [aws.Config] from the given Params. If p.RoleARN is
// set, the base credentials are used to assume that role via STS.
func LoadConfig(ctx context.Context, p Params) (aws.Config, error) {
	var opts []func(*config.LoadOptions) error

	if p.Region != "" {
		opts = append(opts, config.WithRegion(p.Region))
	}

	if p.Endpoint != "" {
		opts = append(opts, config.WithBaseEndpoint(p.Endpoint))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("could not load AWS config: %w", err)
	}

	if p.RoleARN != "" {
		stsClient := sts.NewFromConfig(cfg)
		cfg.Credentials = aws.NewCredentialsCache(
			stscreds.NewAssumeRoleProvider(stsClient, p.RoleARN),
		)
	}

	return cfg, nil
}
