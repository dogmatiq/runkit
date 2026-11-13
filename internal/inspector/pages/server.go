// Package pages serves the inspector's website.
package pages

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dogmatiq/runkit/internal/inspector/observe"
)

// contentSecurityPolicy allows the page to load scripts only from the
// inspector itself and from the CDN that serves htmx, and everything else
// only from the inspector.
const contentSecurityPolicy = "default-src 'none'; " +
	"script-src 'self' https://cdn.jsdelivr.net; " +
	"style-src 'self'; " +
	"img-src 'self' data:; " +
	"connect-src 'self'; " +
	"base-uri 'none'; " +
	"form-action 'self'; " +
	"frame-ancestors 'none'"

// Server serves the inspector's pages.
//
// It serves only GET and HEAD requests. Nothing it serves changes anything.
type Server struct {
	// Application is the application that the inspector is linked with.
	Application *observe.Application

	// Database is the engine's database.
	Database *observe.Database

	// Observer provides the most recent observation.
	Observer *observe.Observer

	// Broadcaster sends live updates to open pages.
	Broadcaster *Broadcaster

	// PageTTL is how long a rendered page is reused before the queries behind
	// it run again.
	PageTTL time.Duration

	once  sync.Once
	mux   *http.ServeMux
	cache pageCache
}

// ServeHTTP serves the page that r asks for.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.once.Do(s.init)

	h := w.Header()
	h.Set("Content-Security-Policy", contentSecurityPolicy)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.Set("Allow", "GET, HEAD")
		http.Error(w, "the inspector only reads", http.StatusMethodNotAllowed)
		return
	}

	s.mux.ServeHTTP(w, r)
}

func (s *Server) init() {
	if s.PageTTL <= 0 {
		panic("page TTL must be positive")
	}

	s.cache.TTL = s.PageTTL
	s.mux = http.NewServeMux()

	s.mux.Handle("GET /static/", http.StripPrefix("/static/", staticHandler()))
	s.mux.HandleFunc("GET /live", s.Broadcaster.serve)

	s.page("GET /{$}", s.overview)
	s.page("GET /nodes", s.nodes)
	s.page("GET /queues", s.queues)
	s.page("GET /queues/{id}", s.queue)
	s.page("GET /messages/{id}", s.message)
	s.page("GET /streams", s.eventStreams)
	s.page("GET /streams/{id}", s.eventStream)
	s.page("GET /handlers", s.handlers)
	s.page("GET /handlers/{key}", s.handler)
	s.page("GET /handlers/{key}/instances/{id}", s.instance)
	s.page("GET /", s.notFound)
}

// page registers a page handler. The handler receives the details that every
// page needs, and its output is cached for [Server.PageTTL].
func (s *Server) page(pattern string, fn func(http.ResponseWriter, *http.Request, frame)) {
	s.mux.HandleFunc(
		pattern,
		func(w http.ResponseWriter, r *http.Request) {
			p := s.cache.Get(
				r.URL.RequestURI(),
				func(w http.ResponseWriter) {
					fn(w, r, s.frame(r))
				},
			)

			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(p.Status)
			w.Write(p.Body)
		},
	)
}

// staticHandler serves the inspector's stylesheet and script.
func staticHandler() http.Handler {
	files := http.FileServerFS(staticFS)

	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "public, max-age=300")
			files.ServeHTTP(w, r)
		},
	)
}

// frame is what every page needs to render its layout.
type frame struct {
	// Title is the page's title.
	Title string

	// Section is the navigation link that the page belongs to.
	Section string

	// Root is the relative URL of the inspector's root, ending in a slash.
	// Every link on the page starts with it, so the inspector works wherever
	// it is mounted.
	Root string

	// Self is the relative URL of the page itself, which the page fetches
	// again when an update arrives.
	Self string

	// App is the application that the inspector is linked with.
	App *observe.Application

	// Obs is the most recent observation, or nil if there has not been one
	// yet.
	Obs *observe.Observation

	// Now is the database's clock, estimated from the most recent
	// observation.
	Now time.Time

	// LocalNow is the inspector's clock. Heartbeat times come from it.
	LocalNow time.Time

	// Issues is the things that need attention, from the most recent
	// observation.
	Issues []issue
}

// frame returns the frame for the given request.
func (s *Server) frame(r *http.Request) frame {
	f := frame{
		Root: rootOf(r.URL.Path),
		App:  s.Application,
		Obs:  s.Observer.Latest(),
	}

	f.Self = f.Root + strings.TrimPrefix(r.URL.RequestURI(), "/")
	f.LocalNow = time.Now()
	f.Now = f.LocalNow

	if f.Obs != nil && f.Obs.Err == nil {
		f.Now = f.Obs.At.Add(time.Since(f.Obs.ReadAt))
	}

	if f.Obs != nil {
		f.Issues = findIssues(s.Application, f.Obs, f.Now)
	}

	return f
}

// rootOf returns the relative URL of the inspector's root from a page with
// the given path.
func rootOf(path string) string {
	depth := strings.Count(path, "/") - 1
	if depth <= 0 {
		return "./"
	}
	return strings.Repeat("../", depth)
}
