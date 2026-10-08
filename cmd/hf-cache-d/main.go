// Command hf-cache-d is the HF cache proxy daemon: it caches Hugging Face
// Hub artifacts in an S3-compatible store and serves them over HTTP.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/metrics"
	"github.com/miadabdi/hf-cache-d/internal/proxy"
	"github.com/miadabdi/hf-cache-d/internal/push"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if cfg.PushToken == "" {
		log.Printf("warning: PUSH_TOKEN is empty, push lane disabled")
	}

	// The store is constructed now to fail fast on missing config; the
	// metadata proxy lane serves from it, later lanes reuse it.
	st, err := store.New(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey)
	if err != nil {
		log.Fatalf("s3 store: %v", err)
	}

	handler, p := newMux(cfg.HFUpstream, cfg.PushToken, st)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Periodic manifest integrity self-check (disabled at interval 0). It
	// shares the shutdown context: srv.Shutdown below outlives it via ctx.
	if cfg.IntegrityCheckInterval > 0 {
		go runIntegrityChecks(ctx, st, p, log.Default(), cfg.IntegrityCheckInterval)
	}

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("hf-cache-d %s listening on %s (upstream %s)", version, cfg.ListenAddr, cfg.HFUpstream)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// newMux builds the HTTP routing table.
//
// ROUTING ORDER CONTRACT: Go 1.22 ServeMux precedence is specificity-based,
// not registration-order-based. The file lane's
// "/{org}/{name}/resolve/{rev}/{file...}" overlaps "/api/models/" (e.g.
// "/api/models/resolve/x/y") with neither pattern more specific, so
// co-registering them on one mux panics at startup regardless of order.
// The file lane therefore lives on a CHILD mux mounted at "/":
//   - /healthz, /api/models/ and /v1/artifacts/ (more specific) win on the
//     parent;
//   - everything else falls through to the file lane, which 404s
//     non-matching shapes (including /api/models/... never reaches it).
//
// /metricsz (Task 5) must also mount on the PARENT mux (as a literal or
// subtree, which always wins over "/"); anything left falls to the file lane.
func newMux(upstream, pushToken string, st *store.Store) (http.Handler, *proxy.Proxy) {
	// *store.Store satisfies muxStore; the indirection exists for the
	// handler tests' in-memory fake.
	return newMuxAny(upstream, pushToken, st)
}

// newMuxAny is newMux over the narrow store interface, so handler tests
// can pass their in-memory fake while production passes *store.Store.
// Returned proxy feeds the integrity self-check's manifest source.
func newMuxAny(upstream, pushToken string, st muxStore) (http.Handler, *proxy.Proxy) {
	// The sealed-local index cache is shared: the proxy reads it (shadowing
	// rule) and the push lane refreshes it on seal.
	ix := local.NewIndexes(st)
	p := proxy.New(upstream, st)
	p.SetLocalIndexes(ix)

	pl := push.New(pushToken, st, ix)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/metricsz", handleMetricsz)
	p.Register(mux)  // /api/models/
	pl.Register(mux) // /v1/artifacts/
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	mux.HandleFunc("/{$}", handleIndex)
	// One middleware around the WHOLE tree: every request is logged and
	// counted exactly once, including the child-mux file lane.
	return logMiddleware(metrics.Default, accessLogger)(mux), p
}

// accessLogger is the process-wide request logger (stderr, plain text).
var accessLogger = log.New(os.Stderr, "", log.LstdFlags|log.LUTC)

// muxStore is the union of the store slices the lanes consume.
type muxStore interface {
	push.Store
	GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": version})
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routes": []string{
		"/healthz",
		"/metricsz",
		"/api/models/{repo}",
		"/api/models/{repo}/revision/{rev}",
		"/api/models/{repo}/tree/{rev}",
		"/v1/artifacts/{repo} (list, anonymous)",
		"/v1/artifacts/{repo}/{version}/{file} (PUT, bearer)",
		"/v1/artifacts/{repo}/{version}/manifest (POST, bearer)",
		"/{repo}/resolve/{rev}/{file}",
		"/",
	}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
