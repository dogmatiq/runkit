package pages

import (
	"fmt"
	"time"

	"github.com/dogmatiq/runkit/internal/inspector/observe"
)

// severity is how urgently a person should look at an issue. Its value is the
// CSS class that shows it.
type severity string

const (
	// broken is the severity of something that has stopped working.
	broken severity = "bad"

	// atRisk is the severity of something that may stop working.
	atRisk severity = "warn"

	// inProgress is the severity of work that the engine has not finished yet.
	inProgress severity = "info"
)

// issue is something that a person watching the cluster should look at.
type issue struct {
	// Severity is how urgently a person should look at the issue.
	Severity severity

	// Text describes the issue.
	Text string

	// Link is the inspector's path to the page about the issue, relative to
	// its root, or empty.
	Link string
}

// IsBroken returns true if the issue is something that has stopped working.
func (i issue) IsBroken() bool {
	return i.Severity == broken
}

// findIssues returns the issues in an observation, the most severe first.
func findIssues(app *observe.Application, obs *observe.Observation, now time.Time) []issue {
	if obs.Err != nil {
		return []issue{
			{
				Severity: broken,
				Text:     obs.Err.Error(),
			},
		}
	}

	var issues []issue

	add := func(s severity, link, format string, args ...any) {
		issues = append(
			issues,
			issue{
				Severity: s,
				Text:     fmt.Sprintf(format, args...),
				Link:     link,
			},
		)
	}

	switch {
	case !obs.LiveNodes.Known:
		add(atRisk, "nodes", "The live nodes are not known yet: %s.", obs.LiveNodes.Reason)
	case len(obs.LiveNodes.Nodes) == 0:
		add(broken, "nodes", "There are no live nodes, so no messages are delivered and no event streams are consumed.")
	}

	switch {
	case obs.BoundApplication == nil:
		add(atRisk, "", "The database is not bound to an application. No engine has run against it yet.")
	case !obs.BoundApplication.GetKey().Equal(app.Identity.GetKey()):
		add(
			broken,
			"",
			"The database is bound to the application %q, but the inspector is linked with %q.",
			obs.BoundApplication.GetName(),
			app.Identity.GetName(),
		)
	}

	for _, err := range []error{obs.QueuesErr, obs.EventStreamsErr} {
		if err != nil {
			add(broken, "", "%s", err)
		}
	}

	if obs.IsQueuesTruncated {
		n := formatNumber(len(obs.Queues))
		add(atRisk, "queues", "There are more than %s queues. The inspector shows the %s with the most messages, and its totals count only those.", n, n)
	}

	if obs.IsEventStreamsTruncated {
		n := formatNumber(len(obs.EventStreams))
		add(atRisk, "streams", "There are more than %s event streams. The inspector shows the first %s, and its totals count only those.", n, n)
	}

	for _, q := range obs.Queues {
		link := "queues/" + q.ID.AsString()

		if q.IsStalled(now) {
			add(
				broken,
				link,
				"Queue %s has %s that are due, and the oldest has been due for %s.",
				shortID(q.ID),
				countOf(q.Due, "message", "messages"),
				formatDuration(q.LongestWait(now)),
			)
		}

		if q.Failing > 0 {
			add(
				atRisk,
				link,
				"Queue %s has %s whose most recent attempt failed. The most failures in a row is %s.",
				shortID(q.ID),
				countOf(q.Failing, "message", "messages"),
				formatNumber(q.MostFailures),
			)
		}
	}

	for _, s := range obs.Handlers {
		link := "handlers/" + s.Handler.Key.AsString()

		for _, err := range []error{s.InstancesErr, s.ProgressErr, s.QueuedMessagesErr, s.CompactionErr} {
			if err != nil {
				add(broken, link, "%s", err)
			}
		}

		if n := s.Unhandled(); n > 0 {
			add(
				inProgress,
				link,
				"%s has %s to handle.",
				s.Handler.Name,
				countOf(n, "event", "events"),
			)
		}
	}

	return sortBySeverity(issues)
}

// sortBySeverity returns the given issues with the broken ones first and the
// ones in progress last, keeping the order of issues with the same severity.
func sortBySeverity(issues []issue) []issue {
	var sorted []issue

	for _, s := range []severity{broken, atRisk, inProgress} {
		for _, i := range issues {
			if i.Severity == s {
				sorted = append(sorted, i)
			}
		}
	}

	return sorted
}
