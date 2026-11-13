package pages

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/protobuf/uuidpb"
)

//go:embed templates/*.html
var templateFiles embed.FS

//go:embed static
var staticFiles embed.FS

// staticFS is the files that the inspector serves under "static/".
var staticFS, _ = fs.Sub(staticFiles, "static")

// templates maps the name of each page's template file to the page's
// template, which includes the shared layout.
var templates = parseTemplates()

func parseTemplates() map[string]*template.Template {
	base := template.Must(
		template.New("").
			Funcs(templateFuncs).
			ParseFS(templateFiles, "templates/_*.html"),
	)

	pages := map[string]*template.Template{}

	names, err := fs.Glob(templateFiles, "templates/[a-z]*.html")
	if err != nil {
		panic(err)
	}

	for _, name := range names {
		t := template.Must(base.Clone())
		template.Must(t.ParseFS(templateFiles, name))
		pages[strings.TrimPrefix(name, "templates/")] = t
	}

	return pages
}

// templateFuncs is the functions that templates may call.
var templateFuncs = template.FuncMap{
	"short":     shortID,
	"id":        func(id *uuidpb.UUID) string { return id.AsString() },
	"num":       formatNumber,
	"compact":   formatCompact,
	"dur":       formatDuration,
	"rel":       relativeTime,
	"seg":       url.PathEscape,
	"query":     url.QueryEscape,
	"icon":      icon,
	"version":   func() string { return pagesVersion },
	"kind":      func(k message.Kind) string { return k.String() },
	"htype":     func(t config.HandlerType) string { return t.String() },
	"route":     formatRouteType,
	"sparkline": sparkline,
	"bindings":  bindingBar,
	"streambar": streamBar,
	"queuebar":  queueBar,
	"add":       func(a, b int) int { return a + b },
	"list":      func(values ...any) []any { return values },
	"plural":    countOf,
	"round":     func(f float64) int { return int(math.Round(f)) },
	"noun":      nounFor,
	"logo":      logo,
	"versions":  func() moduleVersions { return versions },
}

// nounFor returns the singular form of a noun if n is 1, and the plural form
// otherwise.
func nounFor(n any, singular, plural string) string {
	switch n {
	case 1, uint64(1), int64(1):
		return singular
	default:
		return plural
	}
}

// countOf returns n followed by the singular or plural form of a noun, such as
// "1 event" or "2 events".
func countOf(n int, singular, plural string) string {
	if n == 1 {
		return "1 " + singular
	}
	return formatNumber(n) + " " + plural
}

// render writes the page with the given template to w.
func render(w http.ResponseWriter, status int, name string, data any) {
	t, ok := templates[name]
	if !ok {
		panic("unknown template: " + name)
	}

	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, name, data); err != nil {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "<p>Unable to render this page: %s</p>", template.HTMLEscapeString(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	w.Write(buf.Bytes())
}

// shortID returns the first eight characters of a UUID, which is enough to
// tell the IDs on one page apart.
func shortID(id *uuidpb.UUID) string {
	s := id.AsString()
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// formatNumber returns n with a comma between each group of three digits.
func formatNumber(n any) string {
	var s string

	switch n := n.(type) {
	case int:
		s = strconv.Itoa(n)
	case uint64:
		s = strconv.FormatUint(n, 10)
	case int64:
		s = strconv.FormatInt(n, 10)
	default:
		return fmt.Sprint(n)
	}

	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")

	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}

	if negative {
		return "-" + b.String()
	}

	return b.String()
}

// compactUnits is the units that [formatCompact] uses, smallest first.
var compactUnits = []struct {
	Size   float64
	Suffix string
}{
	{1e3, "K"},
	{1e6, "M"},
	{1e9, "B"},
}

// formatCompact returns n as it is if it is below 1,000, and otherwise with one
// decimal place in the largest unit that keeps it below 1,000, such as "1.0K"
// or "12.3M".
func formatCompact(n any) string {
	var x float64

	switch n := n.(type) {
	case int:
		x = float64(n)
	case uint64:
		x = float64(n)
	case int64:
		x = float64(n)
	default:
		return fmt.Sprint(n)
	}

	if x < 1000 {
		return formatNumber(n)
	}

	for i, u := range compactUnits {
		v := x / u.Size

		// A value that would round up to 1000.0 moves to the next unit,
		// unless there is none.
		if v < 999.95 || i == len(compactUnits)-1 {
			return strconv.FormatFloat(v, 'f', 1, 64) + u.Suffix
		}
	}

	return formatNumber(n)
}

// formatDuration returns a short, rounded form of d, such as "4s", "2m 5s" or
// "3h 10m".
func formatDuration(d time.Duration) string {
	if d < 0 {
		d = -d
	}

	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd %dh", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// relativeTime returns a <time> element that shows t relative to now, such as
// "4s ago" or "in 2m 5s". The page's script keeps the text up to date.
//
// The element stores the difference between t and now, not t itself, so the
// text does not depend on the browser's clock agreeing with the database's.
func relativeTime(now, t time.Time) template.HTML {
	if t.IsZero() {
		return "never"
	}

	offset := t.Sub(now)

	return template.HTML(
		fmt.Sprintf(
			`<time datetime="%s" data-offset="%d" title="%s">%s</time>`,
			t.UTC().Format(time.RFC3339Nano),
			offset.Milliseconds(),
			template.HTMLEscapeString(t.UTC().Format("2006-01-02 15:04:05.000 MST")),
			template.HTMLEscapeString(describeOffset(offset)),
		),
	)
}

// describeOffset returns the text of a relative time. The page's script
// produces the same text.
func describeOffset(offset time.Duration) string {
	if offset >= time.Second {
		return "in " + formatDuration(offset)
	}
	if offset > 0 {
		return "in under 1s"
	}
	if offset > -time.Second {
		return "just now"
	}
	return formatDuration(offset) + " ago"
}

// formatRouteType returns the text that the Dogma API uses for a route type,
// such as "HandlesCommand".
func formatRouteType(t config.RouteType) string {
	switch t {
	case config.HandlesCommandRouteType:
		return "handles command"
	case config.HandlesEventRouteType:
		return "handles event"
	case config.ExecutesCommandRouteType:
		return "executes command"
	case config.RecordsEventRouteType:
		return "records event"
	case config.SchedulesDeadlineRouteType:
		return "schedules deadline"
	default:
		return t.String()
	}
}

// sparkline returns an inline SVG chart of the given chart's points over
// [chartWindow], with each point placed by its time, and the chart's end at the
// right-hand edge. The chart is scaled so that the largest value reaches the
// top.
//
// The points from the last [chartLead] are beyond the right-hand edge, and the
// last value continues flat past them. The points begin [chartMargin] before
// the left-hand edge. So the page's script can scroll the chart left as time
// passes, until the next update, and new points scroll in from the right
// without showing a gap at either edge.
//
// While the points begin inside the window, as they do on a page that has just
// loaded, the chart has a lead-in: the line and the area beneath it continue
// to the left of the first point at the same height, and fade to nothing. It
// shows no value, and gives the chart a soft start instead of an abrupt one.
// It is gone once the points fill the window.
func sparkline(c chart) template.HTML {
	const (
		width  = 240.0
		height = 48.0
		pad    = 2.0

		// leadIn is the width of the lead-in, in the chart's own units.
		leadIn = 60.0
	)

	open := fmt.Sprintf(
		`<svg class="spark" viewBox="0 0 %g %g" preserveAspectRatio="none" data-window="%d" aria-hidden="true">`,
		width,
		height,
		chartWindow.Milliseconds(),
	)

	if len(c.Points) == 0 {
		return template.HTML(open + `</svg>`)
	}

	peak := 0.0
	for _, p := range c.Points {
		peak = math.Max(peak, p.Value)
	}
	if peak == 0 {
		peak = 1
	}

	xOf := func(t time.Time) float64 {
		return width - c.End.Sub(t).Seconds()/chartWindow.Seconds()*width
	}
	yOf := func(v float64) float64 {
		return height - pad - (v/peak)*(height-2*pad)
	}

	first := c.Points[0]
	firstX := xOf(first.At)
	startX := firstX

	// A chart with a lead-in takes its fill and stroke from gradients, so its
	// paths have classes that the stylesheet leaves alone. A stylesheet rule
	// would override the paths' attributes.
	var (
		line      strings.Builder
		defs      string
		areaClass = `class="spark-area"`
		lineClass = `class="spark-line"`
	)

	if firstX > 0 {
		startX = firstX - leadIn

		fmt.Fprintf(&line, "M%.2f %.2f L%.2f %.2f", startX, yOf(first.Value), firstX, yOf(first.Value))

		defs = fmt.Sprintf(
			`<defs>`+
				`<linearGradient id="%[1]s-area" gradientUnits="userSpaceOnUse" x1="%[2]g" x2="%[3]g" y1="0" y2="0">`+
				`<stop class="spark-area-out" offset="0"/><stop class="spark-area-in" offset="1"/></linearGradient>`+
				`<linearGradient id="%[1]s-line" gradientUnits="userSpaceOnUse" x1="%[2]g" x2="%[3]g" y1="0" y2="0">`+
				`<stop class="spark-line-out" offset="0"/><stop class="spark-line-in" offset="1"/></linearGradient>`+
				`</defs>`,
			template.HTMLEscapeString(c.ID),
			startX,
			firstX,
		)
		areaClass = fmt.Sprintf(`class="spark-led" fill="url(#%s-area)"`, template.HTMLEscapeString(c.ID))
		lineClass = fmt.Sprintf(`class="spark-led spark-led-line" fill="none" stroke="url(#%s-line)"`, template.HTMLEscapeString(c.ID))
	}

	for i, p := range c.Points {
		switch {
		case i == 0 && firstX > 0:
			// The lead-in has already moved the line to this point.
			continue
		case i == 0:
			fmt.Fprintf(&line, "M%.2f %.2f", xOf(p.At), yOf(p.Value))
		default:
			fmt.Fprintf(&line, " L%.2f %.2f", xOf(p.At), yOf(p.Value))
		}
	}

	last := c.Points[len(c.Points)-1]
	beyond := math.Max(xOf(last.At), width) + width*0.25
	fmt.Fprintf(&line, " L%.2f %.2f", beyond, yOf(last.Value))

	area := fmt.Sprintf(
		"%s L%.2f %g L%.2f %g Z",
		line.String(),
		beyond,
		height,
		startX,
		height,
	)

	return template.HTML(
		fmt.Sprintf(
			`%s%s<g>`+
				`<path %s d="%s"/>`+
				`<path %s d="%s" vector-effect="non-scaling-stroke"/>`+
				`</g></svg>`,
			open,
			defs,
			areaClass,
			area,
			lineClass,
			line.String(),
		),
	)
}
