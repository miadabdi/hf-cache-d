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

	// The store is constructed now to fail fast on missing config, and is
	// consumed by later tasks (proxy/cache lanes wire it into handlers).
	_, err = store.New(cfg.S3Endpoint, cfg.S3Bucket, cfg.S3AccessKey, cfg.S3SecretKey)
	if err != nil {
		log.Fatalf("s3 store: %v", err)
	}

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           newMux(),
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

// newMux builds the HTTP routing table for the service skeleton.
func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/", handleIndex)
	return mux
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		allowMethods(w, http.MethodGet, http.MethodHead)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": version})
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routes": []string{"/healthz", "/"}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func allowMethods(w http.ResponseWriter, methods ...string) {
	for _, m := range methods {
		w.Header().Add("Allow", m)
	}
}
