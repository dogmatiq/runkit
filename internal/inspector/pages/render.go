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
	"sync"
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
	"dur":       formatDuration,
	"rel":       relativeTime,
	"seg":       url.PathEscape,
	"query":     url.QueryEscape,
	"kind":      func(k message.Kind) string { return k.String() },
	"htype":     func(t config.HandlerType) string { return t.String() },
	"route":     formatRouteType,
	"sparkline": sparkline,
	"add":       func(a, b int) int { return a + b },
	"list":      func(values ...any) []any { return values },
	"plural":    countOf,
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
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

// sparkline returns an inline SVG chart of the given values, drawn left to
// right, scaled so that the largest value reaches the top.
func sparkline(values []float64) template.HTML {
	const (
		width  = 240.0
		height = 48.0
		pad    = 2.0
	)

	if len(values) < 2 {
		return template.HTML(
			fmt.Sprintf(
				`<svg class="spark" viewBox="0 0 %g %g" preserveAspectRatio="none" aria-hidden="true"></svg>`,
				width,
				height,
			),
		)
	}

	peak := 0.0
	for _, v := range values {
		peak = math.Max(peak, v)
	}
	if peak == 0 {
		peak = 1
	}

	step := width / float64(len(values)-1)

	var line strings.Builder
	for i, v := range values {
		x := float64(i) * step
		y := height - pad - (v/peak)*(height-2*pad)

		if i == 0 {
			fmt.Fprintf(&line, "M%.1f %.1f", x, y)
		} else {
			fmt.Fprintf(&line, " L%.1f %.1f", x, y)
		}
	}

	area := fmt.Sprintf("%s L%g %g L0 %g Z", line.String(), width, height, height)

	return template.HTML(
		fmt.Sprintf(
			`<svg class="spark" viewBox="0 0 %g %g" preserveAspectRatio="none" aria-hidden="true">`+
				`<path class="spark-area" d="%s"/>`+
				`<path class="spark-line" d="%s" vector-effect="non-scaling-stroke"/>`+
				`</svg>`,
			width,
			height,
			area,
			line.String(),
		),
	)
}

// pageCache holds rendered pages for a short time, so that however many
// people view a page, and however often their browsers refresh it, the
// queries behind it run at most once per [pageCache.TTL].
type pageCache struct {
	// TTL is how long a rendered page is kept.
	TTL time.Duration

	m     sync.Mutex
	pages map[string]cachedPage
}

// cachedPage is a rendered page.
type cachedPage struct {
	Status  int
	Body    []byte
	Expires time.Time
}

// Get returns the rendered page for the given URL, calling render to render
// it if the cache does not hold it.
func (c *pageCache) Get(key string, render func(http.ResponseWriter)) cachedPage {
	if p, ok := c.load(key); ok {
		return p
	}

	rec := &recorder{
		header: http.Header{},
		status: http.StatusOK,
	}
	render(rec)

	p := cachedPage{
		Status:  rec.status,
		Body:    rec.body.Bytes(),
		Expires: time.Now().Add(c.TTL),
	}

	c.store(key, p)

	return p
}

func (c *pageCache) load(key string) (cachedPage, bool) {
	c.m.Lock()
	defer c.m.Unlock()

	p, ok := c.pages[key]
	if !ok || time.Now().After(p.Expires) {
		return cachedPage{}, false
	}

	return p, true
}

func (c *pageCache) store(key string, p cachedPage) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.pages == nil {
		c.pages = map[string]cachedPage{}
	}

	now := time.Now()
	for k, existing := range c.pages {
		if now.After(existing.Expires) {
			delete(c.pages, k)
		}
	}

	c.pages[key] = p
}

// recorder is an [http.ResponseWriter] that keeps what is written to it.
type recorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *recorder) WriteHeader(status int)      { r.status = status }
