package heartbeatpb

import (
	"errors"
	"fmt"
	"uuid"
)

// Validate returns an error if x is not a valid heartbeat.
//
// The node ID of a valid heartbeat is not the nil UUID, and its expiry is
// present and positive. Validate ignores unknown fields.
func (x *Heartbeat) Validate() error {
	if err := x.GetNodeId().Validate(); err != nil {
		return fmt.Errorf("invalid node ID: %w", err)
	}

	// The nil UUID is never the ID of a node. [FindWorkloadOwner] uses it to
	// mean that it hasn't found an owner, so it must not be a live node.
	if x.GetNodeId().AsStd() == uuid.Nil() {
		return errors.New("invalid node ID: the nil UUID is not a node ID")
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
