package pages

import (
	"fmt"
	"html/template"
	"sort"
	"strings"

	"github.com/dogmatiq/runkit/internal/inspector/observe"
)

// barSegments is the largest number of segments that a bar shows. Anything
// smaller than the largest few is merged into one last segment.
const barSegments = 6

// segment is one part of a bar.
type segment struct {
	// Value is the size of the segment, relative to the others.
	Value float64

	// Title describes the segment, and appears as its tooltip.
	Title string

	// IsOther is true for the segment that merges the smaller ones.
	IsOther bool
}

// segmentBar returns an inline SVG bar, like a disk usage bar, whose segments
// are proportional to their values. Segments are drawn largest first.
func segmentBar(class string, segments []segment) template.HTML {
	total := 0.0
	for _, s := range segments {
		total += s.Value
	}

	const gap = 0.6

	var b strings.Builder

	fmt.Fprintf(
		&b,
		`<svg class="segbar %s" viewBox="0 0 100 10" preserveAspectRatio="none" role="img">`,
		template.HTMLEscapeString(class),
	)

	// An empty bar is still drawn, as an empty track, so that it does not move
	// the page when it fills.
	x := 0.0
	for i, s := range segments {
		if total == 0 {
			break
		}

		width := s.Value / total * 100
		drawn := width
		if i < len(segments)-1 {
			drawn -= gap
		}

		name := fmt.Sprintf("seg-%d", i)
		if s.IsOther {
			name = "seg-other"
		}

		if drawn > 0 {
			fmt.Fprintf(
				&b,
				`<rect class="%s" x="%.3f" y="0" width="%.3f" height="10"><title>%s</title></rect>`,
				name,
				x,
				drawn,
				template.HTMLEscapeString(s.Title),
			)
		}

		x += width
	}

	b.WriteString(`</svg>`)

	return template.HTML(b.String())
}

// bindingBar returns a bar of how a handler's instances are spread across the
// things that they are bound to. The class names the subsystem
// of those things, such as "streams", and the nouns name one thing, such as
// "event stream", and many.
func bindingBar(b observe.Bindings, class, singular, plural string) template.HTML {
	var segments []segment

	for i, binding := range b.Bindings {
		if i == barSegments-1 && len(b.Bindings) > barSegments {
			rest := 0
			for _, r := range b.Bindings[i:] {
				rest += r.Instances
			}

			segments = append(
				segments,
				segment{
					Value:   float64(rest),
					Title:   fmt.Sprintf("%s on %d other %s", countOf(rest, "instance", "instances"), len(b.Bindings)-i, plural),
					IsOther: true,
				},
			)
			break
		}

		title := fmt.Sprintf("%s with no %s yet", countOf(binding.Instances, "instance", "instances"), singular)
		if binding.ID != nil {
			title = fmt.Sprintf("%s on %s %s", countOf(binding.Instances, "instance", "instances"), singular, shortID(binding.ID))
		}

		segments = append(
			segments,
			segment{
				Value: float64(binding.Instances),
				Title: title,
			},
		)
	}

	return segmentBar(class, segments)
}

// streamBar returns a bar of the sizes of the given event streams, like a disk
// usage bar.
func streamBar(streams []observe.EventStream) template.HTML {
	sorted := append([]observe.EventStream(nil), streams...)
	sort.Slice(
		sorted,
		func(i, j int) bool {
			return sorted[i].Length > sorted[j].Length
		},
	)

	var segments []segment

	for i, s := range sorted {
		if i == barSegments-1 && len(sorted) > barSegments {
			var rest uint64
			for _, r := range sorted[i:] {
				rest += r.Length
			}

			segments = append(
				segments,
				segment{
					Value:   float64(rest),
					Title:   fmt.Sprintf("%s events on %d other event streams", formatCompact(rest), len(sorted)-i),
					IsOther: true,
				},
			)
			break
		}

		segments = append(
			segments,
			segment{
				Value: float64(s.Length),
				Title: fmt.Sprintf("%s events on event stream %s", formatCompact(s.Length), shortID(s.ID)),
			},
		)
	}

	return segmentBar("streams", segments)
}

// queueBar returns a bar of the number of messages in each of the given queues.
func queueBar(queues []observe.Queue) template.HTML {
	var segments []segment

	for i, q := range queues {
		if i == barSegments-1 && len(queues) > barSegments {
			rest := 0
			for _, r := range queues[i:] {
				rest += r.Messages
			}

			segments = append(
				segments,
				segment{
					Value:   float64(rest),
					Title:   fmt.Sprintf("%s in %d other queues", countOf(rest, "message", "messages"), len(queues)-i),
					IsOther: true,
				},
			)
			break
		}

		segments = append(
			segments,
			segment{
				Value: float64(q.Messages),
				Title: fmt.Sprintf("%s in queue %s", countOf(q.Messages, "message", "messages"), shortID(q.ID)),
			},
		)
	}

	return segmentBar("queues", segments)
}

// nodeBar returns a bar with one segment for each live node, as the inspector
// computes ownership. A segment's width is the node's share of the total, and
// a node with a count of zero has a square segment. The describe function
// returns the tooltip of each segment.
//
// Each segment is an <svg> element because its width attribute sets its width
// without a style attribute, which the Content-Security-Policy forbids. The
// widths add up to 100%, and the bar shrinks them in proportion to make room
// for the gaps and the square segments.
func nodeBar(counts []observe.NodeCount, describe func(observe.NodeCount) string) template.HTML {
	total := 0
	for _, c := range counts {
		total += c.Count
	}

	var b strings.Builder
	b.WriteString(`<div class="nodebar">`)

	for i, c := range counts {
		// The segments are shaded like those of the other bars, in the order
		// of the nodes, and the shades repeat after barSegments of them, so
		// that neighbors differ.
		width := "8"
		class := fmt.Sprintf("nodeseg seg-%d is-idle", i%barSegments)

		if c.Count > 0 {
			width = fmt.Sprintf("%.3f%%", float64(c.Count)/float64(total)*100)
			class = fmt.Sprintf("nodeseg seg-%d", i%barSegments)
		}

		fmt.Fprintf(
			&b,
			`<svg class="%s" width="%s" height="8" role="img"><title>%s</title></svg>`,
			class,
			width,
			template.HTMLEscapeString(describe(c)),
		)
	}

	b.WriteString(`</div>`)

	return template.HTML(b.String())
}
