package cluster

import (
	"encoding/base64"
	"fmt"

	"github.com/dogmatiq/runkit/internal/cluster/internal/heartbeatpb"
)

// encodeHeartbeat returns the given heartbeat as the payload of a PostgreSQL
// notification.
//
// The payload is text, so it is the base64 encoding of the binary form of the
// message. It returns an error if the heartbeat is not valid.
func encodeHeartbeat(h *heartbeatpb.Heartbeat) (string, error) {
	if err := h.Validate(); err != nil {
		return "", fmt.Errorf("invalid heartbeat: %w", err)
	}

	data, err := h.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("unable to marshal heartbeat: %w", err)
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

// decodeHeartbeat returns the heartbeat in the payload of a PostgreSQL
// notification.
//
// It ignores unknown fields in the payload, and returns an error if the payload
// is not a valid heartbeat.
func decodeHeartbeat(payload string) (*heartbeatpb.Heartbeat, error) {
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("heartbeat is not valid base64: %w", err)
	}

	h := &heartbeatpb.Heartbeat{}
	if err := h.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("unable to unmarshal heartbeat: %w", err)
	}

	if err := h.Validate(); err != nil {
		return nil, fmt.Errorf("invalid heartbeat: %w", err)
	}

	return h, nil
}
