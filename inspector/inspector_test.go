package inspector_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/inspector"
	"github.com/dogmatiq/runkit/internal/inspector/demoapp"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func TestInspector(t *testing.T) {
	t.Run("it serves every page as a role that may only read, without sending a notification", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		app := &demoapp.App{
			StatementInterval: 50 * time.Millisecond,
			MailerFailureRate: 0.5,
		}

		runBank(t, db, app)

		readerDB := openReader(t, db)

		insp, err := New(app, readerDB)
		if err != nil {
			t.Fatal(err)
		}

		xtesting.GoE(t, insp.Run)

		server := httptest.NewServer(insp)
		defer server.Close()

		expectNoNotifications(
			t,
			db,
			func() {
				xtesting.WaitFor(
					t,
					"the first observation",
					func() bool {
						return strings.Contains(get(t, server.URL+"/").Body, "Busiest queues")
					},
				)

				pages := crawl(t, server.URL+"/")

				if len(pages) < 10 {
					t.Fatalf("expected to visit at least 10 pages, visited %d", len(pages))
				}
			},
		)
	})

	t.Run("it rejects a request that is not GET or HEAD", func(t *testing.T) {
		db := xtesting.NewDatabase(t)

		insp, err := New(&demoapp.App{}, db)
		if err != nil {
			t.Fatal(err)
		}

		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			req := httptest.NewRequest(method, "/", nil)
			rec := httptest.NewRecorder()

			insp.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s: got status %d, want %d", method, rec.Code, http.StatusMethodNotAllowed)
			}
		}
	})
}

// runBank runs the bank application on a cluster until it has opened some
// accounts and sent some statements, then stops the cluster.
func runBank(t *testing.T, db *sql.DB, app *demoapp.App) {
	t.Helper()

	cluster := xtesting.StartCluster(t, db, app, 2)

	for i := range 5 {
		id := fmt.Sprintf("acct-%d", i)

		for _, c := range []dogma.Command{
			&demoapp.OpenAccount{
				AccountID: id,
				Name:      "Alex",
			},
			&demoapp.Deposit{
				AccountID: id,
				Amount:    1000,
			},
			&demoapp.Withdraw{
				AccountID: id,
				Amount:    5000,
			},
		} {
			if err := cluster.ExecuteCommand(t.Context(), c); err != nil {
				t.Fatal(err)
			}
		}
	}

	xtesting.WaitFor(
		t,
		"statements to be sent",
		func() bool {
			var n int
			if err := db.QueryRowContext(
				t.Context(),
				`SELECT count(*) FROM process.instances`,
			).Scan(&n); err != nil {
				t.Fatal(err)
			}
			return n == 5
		},
	)

	cluster.Stop()
}

// openReader returns a connection pool for a new role that may only read the
// engine's tables in the database that db connects to.
func openReader(t *testing.T, db *sql.DB) *sql.DB {
	t.Helper()

	config := connConfig(t, db)
	role := "inspector-test-" + uuidpb.Generate().AsString()[:8]
	ident := pgx.Identifier{role}.Sanitize()

	statements := []string{
		`CREATE ROLE ` + ident + ` LOGIN PASSWORD 'password'`,
		`GRANT CONNECT ON DATABASE ` + pgx.Identifier{config.Database}.Sanitize() + ` TO ` + ident,
	}

	for _, schema := range []string{
		"application",
		"eventstream",
		"messagequeue",
		"aggregate",
		"process",
		"projection",
	} {
		statements = append(
			statements,
			`GRANT USAGE ON SCHEMA `+schema+` TO `+ident,
			`GRANT SELECT ON ALL TABLES IN SCHEMA `+schema+` TO `+ident,
		)
	}

	for _, s := range statements {
		if _, err := db.ExecContext(t.Context(), s); err != nil {
			t.Fatal(err)
		}
	}

	config.User = role
	config.Password = "password"

	reader := stdlib.OpenDB(*config)

	t.Cleanup(func() {
		reader.Close()

		// Roles belong to the whole server, not to the test's database.
		ctx := context.Background()
		db.ExecContext(ctx, `DROP OWNED BY `+ident)
		db.ExecContext(ctx, `DROP ROLE `+ident)
	})

	return reader
}

// connConfig returns the connection configuration of db.
func connConfig(t *testing.T, db *sql.DB) *pgx.ConnConfig {
	t.Helper()

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var config *pgx.ConnConfig

	if err := conn.Raw(
		func(raw any) error {
			config = raw.(*stdlib.Conn).Conn().Config().Copy()
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}

	return config
}

// expectNoNotifications calls fn, and fails the test if any notification is
// sent on the engine's signal channel while it runs.
func expectNoNotifications(t *testing.T, db *sql.DB, fn func()) {
	t.Helper()

	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Raw(
		func(raw any) error {
			pgConn := raw.(*stdlib.Conn).Conn()

			if _, err := pgConn.Exec(t.Context(), `LISTEN "signal"`); err != nil {
				return err
			}
			defer pgConn.Exec(context.Background(), `UNLISTEN "signal"`)

			fn()

			// Notifications that arrived while fn ran are buffered by the
			// connection, so a short wait finds them.
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()

			n, err := pgConn.WaitForNotification(ctx)
			if err == nil {
				return fmt.Errorf("unexpected notification: %q", n.Payload)
			}

			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
}

// page is a page that the test fetched.
type page struct {
	Status int
	Body   string
}

func get(t *testing.T, u string) page {
	t.Helper()

	res, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}

	return page{
		Status: res.StatusCode,
		Body:   string(body),
	}
}

// links matches the href attribute of each link on a page.
var links = regexp.MustCompile(`href="([^"]+)"`)

// crawl visits every page that is reachable from start, and fails the test if
// a page does not load, or shows an error. It returns the visited URLs.
func crawl(t *testing.T, start string) []string {
	t.Helper()

	base, err := url.Parse(start)
	if err != nil {
		t.Fatal(err)
	}

	visited := map[string]bool{}
	queue := []*url.URL{base}

	for len(queue) > 0 && len(visited) < 200 {
		u := queue[0]
		queue = queue[1:]

		key := u.String()
		if visited[key] {
			continue
		}
		visited[key] = true

		p := get(t, key)

		// A link to the cause of a message leads to a command that may have
		// been delivered and deleted. The page says so.
		isDeletedMessage := p.Status == http.StatusNotFound &&
			strings.Contains(p.Body, "deletes a command or deadline once it has been delivered")

		if p.Status != http.StatusOK && !isDeletedMessage {
			t.Fatalf("%s: got status %d", key, p.Status)
		}

		if strings.Contains(p.Body, `class="error"`) {
			t.Fatalf("%s: the page shows an error:\n%s", key, p.Body)
		}

		for _, m := range links.FindAllStringSubmatch(p.Body, -1) {
			ref, err := url.Parse(strings.ReplaceAll(m[1], "&amp;", "&"))
			if err != nil {
				t.Fatal(err)
			}

			next := u.ResolveReference(ref)
			next.Fragment = ""

			if next.Host != base.Host || strings.Contains(next.Path, "/static/") {
				continue
			}

			queue = append(queue, next)
		}
	}

	var urls []string
	for u := range visited {
		urls = append(urls, u)
	}

	return urls
}
