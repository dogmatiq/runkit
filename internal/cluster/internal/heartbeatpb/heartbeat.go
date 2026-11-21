package heartbeatpb

import (
	"errors"
	"fmt"
)

// Validate returns an error if x is not a valid heartbeat.
//
// The node ID of a valid heartbeat is a valid UUID, which excludes the nil UUID,
// and its expiry is present and positive. Validate ignores unknown fields.
func (x *Heartbeat) Validate() error {
	if err := x.GetNodeId().Validate(); err != nil {
		return fmt.Errorf("invalid node ID: %w", err)
	}

	if x.GetExpiresAfter() == nil {
		return errors.New("no expiry")
	}

	if err := x.GetExpiresAfter().CheckValid(); err != nil {
		return fmt.Errorf("invalid expiry: %w", err)
	}

	if x.GetExpiresAfter().AsDuration() <= 0 {
		return errors.New("invalid expiry: it is not positive")
	}

	return nil
}
