// observe.go holds hf-cache-d's observability pieces: the request-logging
// middleware, the /metricsz exposition endpoint, and the periodic manifest
// integrity self-check.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/manifest"
	"github.com/miadabdi/hf-cache-d/internal/metrics"
)

// countersWriter snapshots the response status and counts the body bytes
// written, passing everything through to the wrapped writer. Flush is
// forwarded: the file lane's streaming cold pulls depend on it.
type countersWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (cw *countersWriter) WriteHeader(code int) {
	cw.status = code
	cw.ResponseWriter.WriteHeader(code)
}

func (cw *countersWriter) Write(p []byte) (int, error) {
	n, err := cw.ResponseWriter.Write(p)
	cw.bytes += int64(n)
	return n, err
}

func (cw *countersWriter) Flush() {
	if f, ok := cw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// routeOf classifies a request path for the requests_total{route} label.
// The label vocabulary is fixed: metadata, file, push, healthz, metricsz,
// index, other.
func routeOf(path string) string {
	switch {
	case path == "/healthz":
		return "healthz"
	case path == "/metricsz":
		return "metricsz"
	case path == "/":
		return "index"
	case strings.HasPrefix(path, "/api/models"):
		return "metadata"
	case strings.HasPrefix(path, "/v1/artifacts/"):
		return "push"
	case resolvePathRe.MatchString(path):
		// The file lane's /{org}/{name}/resolve/{rev}/{file...}. Counted by
		// path shape, not by which handler answered: a 404 on a
		// resolve-shaped path is still a file-lane request.
		return "file"
	default:
		return "other"
	}
}

var resolvePathRe = regexp.MustCompile(`^/[^/]+/[^/]+/resolve/`)

// logMiddleware wraps next with one-line-per-request logging to logger and
// request/byte accounting in c. The line carries method, path, status,
// bytes, duration, cache status and @commit when the handler set them.
// Only shape-safe request facts are logged: method and bare path — never
// headers (Authorization/Cookie), never the query string.
func logMiddleware(c *metrics.Counters, logger *log.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.AddRequest(routeOf(r.URL.Path)) // at entry: a scrape sees itself
			start := time.Now()
			cw := &countersWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(cw, r)

			cache, commit := cw.Header().Get("X-Cache"), cw.Header().Get("X-Repo-Commit")
			at := ""
			if commit != "" {
				at = "@" + commit
			}
			logger.Printf("%s %s %d %dB %s cache=%s%s",
				r.Method, r.URL.Path, cw.status, cw.bytes,
				time.Since(start).Round(time.Microsecond), cache, at)
			c.AddBytesServed(cw.bytes)
		})
	}
}

// handleMetricsz serves the Prometheus text exposition of the counters.
func handleMetricsz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body := metrics.Default.Render()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = io.WriteString(w, body)
	}
}

// integrityStore is the store slice the self-check needs.
type integrityStore interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
}

// manifestSource supplies the manifest keys eligible for verification. The
// proxy implements it over its read cache; there is deliberately no S3
// listing in the store contract.
// ponytail: discovery limited to recently-served manifests — add store-side
// listing if full-bucket sweeps are ever needed.
type manifestSource interface {
	ManifestKeys() []string
}

// runIntegrityChecks periodically verifies ONE random manifest's files:
// sha256 of the stored bytes vs the manifest's recorded digest. A mismatch
// logs a line starting "CRITICAL: integrity mismatch:" and NEVER deletes
// anything — surfacing is the whole job, remediation stays human.
// interval <= 0 disables the loop (it returns immediately). Shutdown:
// cancel ctx.
func runIntegrityChecks(ctx context.Context, st integrityStore, src manifestSource, logger *log.Logger, interval time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			keys := src.ManifestKeys()
			if len(keys) == 0 {
				continue
			}
			key := keys[rand.Intn(len(keys))]
			if err := checkOneManifest(ctx, st, logger, key); err != nil {
				logger.Printf("integrity self-check on %s aborted: %v", key, err)
			}
		}
	}
}

// checkOneManifest verifies every file entry of the manifest at key.
// A manifest that cannot be read or decoded aborts the check (reported to
// the caller, logged as aborted) — that is a backend fault, not evidence of
// corruption. Missing/unreadable FILE OBJECTS under an existing manifest
// ARE integrity failures and log CRITICAL.
func checkOneManifest(ctx context.Context, st integrityStore, logger *log.Logger, key string) error {
	rc, _, err := st.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("manifest read: %w", err)
	}
	defer rc.Close()
	var m manifest.Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return fmt.Errorf("manifest decode: %w", err)
	}
	if len(m.Files) == 0 {
		return nil // nothing recorded (mid-publish snapshot): healthy
	}

	// Object keys derive from the manifest's own key and identity:
	//   public pull: pub/<xx>/<sha>/manifest.json + "hf:<repo>@<sha>"
	//     → pub/<xx>/<sha>/<repo>/<file>
	//   seal:        priv/<org>/<name>/<version>/manifest.json
	//     + "private:<repo>@<version>" → priv/<org>/<name>/<version>/files/<file>
	prefix := strings.TrimSuffix(strings.TrimSuffix(key, "manifest.json"), "/")
	private := strings.HasPrefix(m.Identity, "private:")
	repo, ok := identityRepo(m.Identity)
	if !ok && !private {
		return nil // unknown identity shape: not ours to verify
	}

	for file, want := range m.Files {
		objKey := prefix + "/" + file
		if private {
			objKey = prefix + "/files/" + file
		} else {
			objKey = prefix + "/" + repo + "/" + file
		}
		got, err := hashObject(ctx, st, objKey)
		if err != nil {
			logger.Printf("CRITICAL: integrity mismatch: manifest %s lists %s but the object is missing/unreadable: %v", key, objKey, err)
			continue
		}
		if got != want {
			logger.Printf("CRITICAL: integrity mismatch: object %s hashes to %s, manifest %s says %s", objKey, got, key, want)
		}
	}
	return nil
}

// hashObject streams one object through sha256.
func hashObject(ctx context.Context, st integrityStore, key string) (string, error) {
	rc, _, err := st.Get(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	got, _, err := manifest.HashReader(rc)
	return got, err
}

// identityRepo extracts the repo from "hf:<repo>@<sha>". ok=false for any
// other identity shape.
func identityRepo(identity string) (repo string, ok bool) {
	rest, found := strings.CutPrefix(identity, "hf:")
	if !found {
		return "", false
	}
	repo, _, found = strings.Cut(rest, "@")
	return repo, found && repo != ""
}
