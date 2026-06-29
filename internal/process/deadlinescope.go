package process

import "time"

type deadlineScope struct {
	messageScope
	scheduledFor time.Time
}

func (s *deadlineScope) ScheduledFor() time.Time {
	return s.scheduledFor
}
