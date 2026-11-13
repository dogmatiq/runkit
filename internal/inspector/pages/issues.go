package pages

import (
	"fmt"
	"time"

	"github.com/dogmatiq/runkit/internal/inspector/observe"
)

// issue is something that a person watching the cluster should look at.
type issue struct {
	// Severity is "bad" for something that is broken, "warn" for something
	// that may become a problem, or "info" for work in progress.
	Severity string

	// Text describes the issue.
	Text string

	// Link is the inspector's path to the page about the issue, relative to
	// its root, or empty.
	Link string
}

// findIssues returns the issues in an observation, the most severe first.
func findIssues(app *observe.Application, obs *observe.Observation, now time.Time) []issue {
	var bad, warn, info []issue

	if obs.Err != nil {
		return []issue{
			{
				Severity: "bad",
				Text:     obs.Err.Error(),
			},
		}
	}

	switch {
	case !obs.Membership.Known:
		warn = append(warn, issue{
			Severity: "warn",
			Text:     fmt.Sprintf("The live nodes are not known yet: %s.", obs.Membership.Reason),
			Link:     "nodes",
		})
	case len(obs.Membership.Nodes) == 0:
		bad = append(bad, issue{
			Severity: "bad",
			Text:     "There are no live nodes, so no messages are delivered and no event streams are consumed.",
			Link:     "nodes",
		})
	}

	switch {
	case obs.BoundApplication == nil:
		warn = append(warn, issue{
			Severity: "warn",
			Text:     "The database is not bound to an application. No engine has run against it yet.",
		})
	case !obs.BoundApplication.GetKey().Equal(app.Identity.GetKey()):
		bad = append(bad, issue{
			Severity: "bad",
			Text: fmt.Sprintf(
				"The database is bound to the application %q, but the inspector is linked with %q.",
				obs.BoundApplication.GetName(),
				app.Identity.GetName(),
			),
		})
	}

	for _, err := range []error{obs.QueuesErr, obs.EventStreamsErr} {
		if err != nil {
			bad = append(bad, issue{
				Severity: "bad",
				Text:     err.Error(),
			})
		}
	}

	for _, q := range obs.Queues {
		link := "queues/" + q.ID.AsString()

		if q.IsStalled(now) {
			bad = append(bad, issue{
				Severity: "bad",
				Text: fmt.Sprintf(
					"Queue %s has %s that are due, and the oldest has been due for %s.",
					shortID(q.ID),
					countOf(q.Due, "message", "messages"),
					formatDuration(q.LongestWait(now)),
				),
				Link: link,
			})
		}

		if q.Failing > 0 {
			warn = append(warn, issue{
				Severity: "warn",
				Text: fmt.Sprintf(
					"Queue %s has %s whose most recent attempt failed. The most failures in a row is %s.",
					shortID(q.ID),
					countOf(q.Failing, "message", "messages"),
					formatNumber(q.MostFailures),
				),
				Link: link,
			})
		}
	}

	for _, s := range obs.Handlers {
		link := "handlers/" + s.Handler.Key.AsString()

		for _, err := range []error{s.InstancesErr, s.ProgressErr, s.CompactionErr} {
			if err != nil {
				bad = append(bad, issue{
					Severity: "bad",
					Text:     err.Error(),
					Link:     link,
				})
			}
		}

		if n := s.Unhandled(); n > 0 {
			info = append(info, issue{
				Severity: "info",
				Text: fmt.Sprintf(
					"%s has %s to handle.",
					s.Handler.Name,
					countOf(n, "event", "events"),
				),
				Link: link,
			})
		}
	}

	return append(append(bad, warn...), info...)
}
