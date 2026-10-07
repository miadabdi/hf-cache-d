// Command hf-cache-d is the HF cache proxy daemon: it caches Hugging Face
// Hub artifacts in an S3-compatible store and serves them over HTTP.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/proxy"
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

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           newMux(proxy.New(cfg.HFUpstream, st)),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
//   - /healthz and /api/models/ (more specific) win on the parent;
//   - everything else falls through to the file lane, which 404s
//     non-matching shapes (including /api/models/... never reaches it).
//
// /metricsz (Task 5) and the private push lane (Task 4) must mount on the
// PARENT mux before "/" — or as exact/longer literals, which always win.
func newMux(p *proxy.Proxy) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	p.Register(mux) // /api/models/
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	mux.HandleFunc("/{$}", handleIndex)
	return mux
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
		"/api/models/{repo}",
		"/api/models/{repo}/revision/{rev}",
		"/api/models/{repo}/tree/{rev}",
		"/{repo}/resolve/{rev}/{file}",
		"/",
	}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
