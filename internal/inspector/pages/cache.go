package pages

import (
	"bytes"
	"net/http"
	"sync"
	"time"
)

// pageCache holds rendered pages for a short time. However many people view a
// page, and however often their browsers fetch it, the queries behind it run
// at most once per [pageCache.TTL] for each URL. A request for a page that is
// being rendered waits for that render.
type pageCache struct {
	// TTL is how long a rendered page is reused.
	TTL time.Duration

	// m guards pages, and the fields of each page other than Rendered.
	m sync.Mutex

	// pages maps each URL to its rendered page, or to its render in progress.
	pages map[string]*cachedPage
}

// Get returns the rendered page for the given URL. If the cache does not hold
// it, and no render is in progress, it calls render to render it.
func (c *pageCache) Get(key string, render func(http.ResponseWriter)) *cachedPage {
	p, isRenderer := c.claim(key)
	if !isRenderer {
		<-p.Rendered
		return p
	}

	rec := &recorder{
		Headers: http.Header{},
		Status:  http.StatusOK,
	}
	render(rec)

	c.finish(p, rec)

	return p
}

// claim returns the cached page for the given URL. isRenderer is true if the
// caller must render it.
func (c *pageCache) claim(key string) (p *cachedPage, isRenderer bool) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.pages == nil {
		c.pages = map[string]*cachedPage{}
	}

	now := time.Now()

	for k, existing := range c.pages {
		if existing.isExpired(now) {
			delete(c.pages, k)
		}
	}

	if p, ok := c.pages[key]; ok {
		return p, false
	}

	p = &cachedPage{
		Rendered: make(chan struct{}),
	}
	c.pages[key] = p

	return p, true
}

// finish stores the page that rec holds.
func (c *pageCache) finish(p *cachedPage, rec *recorder) {
	c.m.Lock()
	defer c.m.Unlock()

	p.Status = rec.Status
	p.Body = rec.Body.Bytes()
	p.Expires = time.Now().Add(c.TTL)

	// A server error, such as a timeout, is not reused, so that the next
	// request tries again.
	if p.Status >= http.StatusInternalServerError {
		p.Expires = time.Now()
	}

	close(p.Rendered)
}

// cachedPage is a rendered page, or a render in progress.
type cachedPage struct {
	// Rendered is closed once the page is rendered and its other fields are
	// set.
	Rendered chan struct{}

	// Status is the page's HTTP status code.
	Status int

	// Body is the page's HTML.
	Body []byte

	// Expires is the time after which the page is rendered again. It is zero
	// while the page is being rendered.
	Expires time.Time
}

// isExpired returns true if the page has been rendered and has expired at the
// given time.
func (p *cachedPage) isExpired(now time.Time) bool {
	if p.Expires.IsZero() {
		return false
	}
	return !now.Before(p.Expires)
}

// recorder is an [http.ResponseWriter] that keeps what is written to it.
type recorder struct {
	// Headers is the response's headers.
	Headers http.Header

	// Status is the response's HTTP status code.
	Status int

	// Body is the response's body.
	Body bytes.Buffer
}

func (r *recorder) Header() http.Header         { return r.Headers }
func (r *recorder) Write(b []byte) (int, error) { return r.Body.Write(b) }
func (r *recorder) WriteHeader(status int)      { r.Status = status }
