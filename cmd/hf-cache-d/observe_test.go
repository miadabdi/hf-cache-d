package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/metrics"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// errNotFound mirrors the real store's not-found sentinel so the lanes'
// errors.Is comparisons classify misses correctly.
var errNotFound = store.ErrNotFound

// memStore is the cmd-level in-memory store fake (the proxy package's cannot
// be imported). Sufficient for the metrics/integrity/middleware tests here.
type memStore struct {
	mu      sync.Mutex
	objs    map[string][]byte
	failGet bool // every Get/Head returns a transport error (wedge fixture)
}

func newMemStore() *memStore { return &memStore{objs: map[string][]byte{}} }

func (m *memStore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failGet {
		return nil, 0, errors.New("memstore: simulated wedge")
	}
	b, ok := m.objs[key]
	if !ok {
		return nil, 0, errNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}
func (m *memStore) GetRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failGet {
		return nil, errors.New("memstore: simulated wedge")
	}
	b, ok := m.objs[key]
	if !ok {
		return nil, errNotFound
	}
	return io.NopCloser(bytes.NewReader(b[start:end])), nil
}
func (m *memStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = b
	return nil
}
func (m *memStore) Head(_ context.Context, key string) (bool, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failGet {
		return false, 0, errors.New("memstore: simulated wedge")
	}
	b, ok := m.objs[key]
	return ok, int64(len(b)), nil
}

// corrupt replaces an object's bytes (integrity self-check fixture).
func (m *memStore) corrupt(key string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = body
}

func TestVanishedSealedObjectDoesNotLogHit(t *testing.T) {
	st := newMemStore()
	sha := goodSHA
	body := []byte("sealed bytes")
	sum := sha256.Sum256(body)
	key := fmt.Sprintf("pub/%s/%s/org/name/f.bin", sha[:2], sha)
	putManifest(t, st, sha, map[string]string{"f.bin": hex.EncodeToString(sum[:])})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer up.Close()
	previous := metrics.Default
	metrics.Default = &metrics.Counters{}
	defer func() { metrics.Default = previous }()
	mux, _ := newMuxAny(up.URL, "", st, 0)
	var lines bytes.Buffer
	wrapped := logMiddleware(&metrics.Counters{}, log.New(&lines, "", 0))(mux)
	req := httptest.NewRequest(http.MethodHead, "/org/name/resolve/"+sha+"/f.bin", nil)
	resp := httptest.NewRecorder()
	wrapped.ServeHTTP(resp, req)
	if strings.Contains(lines.String(), "cache=HIT") || resp.Header().Get("X-Cache") == "HIT" || resp.Header().Get("ETag") != "" {
		t.Fatalf("vanished object %s advertised hit: headers %v log %q", key, resp.Header(), lines.String())
	}
}

// putManifest installs a release manifest for sha with file sums.
func putManifest(t *testing.T, st *memStore, sha string, files map[string]string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"identity": "hf:org/name@" + sha, "files": files})
	if err := st.Put(context.Background(), fmt.Sprintf("pub/%s/%s/org/name/@manifest", sha[:2], sha), bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
}

const goodSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// TestMetricszCountsAcrossRequests drives the mux with a fake upstream and
// asserts the counters advance and /metricsz serves parseable exposition.
func TestMetricszCountsAcrossRequests(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/models/") {
			fmt.Fprintf(w, `{"sha":%q,"siblings":[{"rfilename":"f.bin"}]}`, goodSHA)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(up.Close)

	st := newMemStore()
	mux, _ := newMuxAny(up.URL, "sekrit", st, 0)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Two metadata requests: first MISS (fetched), second HIT (cached).
	httpGetBody(t, srv.URL+"/api/models/org/name/revision/"+goodSHA)
	httpGetBody(t, srv.URL+"/api/models/org/name/revision/"+goodSHA)

	// A push (bytes pulled) and a metrics scrape.
	pushOne(t, srv.URL, "org/m", "v1", "weights.bin", []byte("0123456789"))

	resp, body := httpGetBody(t, srv.URL+"/metricsz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metricsz status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("metricsz Content-Type = %q", ct)
	}
	for _, want := range []string{
		`hf_cache_requests_total{route="metadata"} 2`,
		`hf_cache_requests_total{route="push"} 1`,
		`hf_cache_requests_total{route="metricsz"} 1`,
		"hf_cache_hits_total 1",
		"hf_cache_misses_total 1",
		"hf_cache_bytes_pulled_upstream_total ", // >0; exact value depends on JSON length
		"hf_cache_upstream_errors_total 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metricsz output missing %q:\n%s", want, body)
		}
	}

	// Upstream failure advances the error counter (and adds a miss).
	upFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(upFail.Close)
	mux2, _ := newMuxAny(upFail.URL, "sekrit", newMemStore(), 0)
	srv2 := httptest.NewServer(mux2)
	t.Cleanup(srv2.Close)
	httpGetBody(t, srv2.URL+"/api/models/org/other/revision/"+goodSHA)
	_, body2 := httpGetBody(t, srv2.URL+"/metricsz")
	if !strings.Contains(body2, "hf_cache_upstream_errors_total 1") {
		t.Errorf("upstream error not counted:\n%s", body2)
	}
}

// TestIntegrityCheckCriticalOnCorruption: a corrupted object under a valid
// manifest must log a CRITICAL line within a short interval, and the object
// must NOT be deleted.
func TestIntegrityCheckCriticalOnCorruption(t *testing.T) {
	st := newMemStore()
	file := []byte("intact weights")
	sum := sha256.Sum256(file)
	key := fmt.Sprintf("pub/%s/%s/org/name/w.bin", goodSHA[:2], goodSHA)
	if err := st.Put(context.Background(), key, bytes.NewReader(file), int64(len(file))); err != nil {
		t.Fatal(err)
	}
	putManifest(t, st, goodSHA, map[string]string{"w.bin": hex.EncodeToString(sum[:])})

	var buf syncBuffer
	logger := log.New(&buf, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runIntegrityChecks(ctx, st, fixedKeys{[]string{fmt.Sprintf("pub/%s/%s/org/name/@manifest", goodSHA[:2], goodSHA)}}, logger, 50*time.Millisecond)

	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(buf.String(), "CRITICAL: integrity mismatch") && time.Now().Before(deadline) {
		st.corrupt(key, []byte("tampered")) // keep it corrupted for the next pass
		time.Sleep(10 * time.Millisecond)
	}
	out := buf.String()
	if !strings.Contains(out, "CRITICAL: integrity mismatch") {
		t.Fatalf("no CRITICAL line within deadline; log:\n%s", out)
	}
	if !strings.Contains(out, key) {
		t.Errorf("CRITICAL line does not name the object key:\n%s", out)
	}
	// Never deletes.
	rc, _, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("integrity check deleted the object: %v", err)
	}
	rc.Close()
}

// TestIntegrityCheckDisabled: interval 0 must never run a pass.
func TestIntegrityCheckDisabled(t *testing.T) {
	st := newMemStore()
	var buf syncBuffer
	logger := log.New(&buf, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { runIntegrityChecks(ctx, st, fixedKeys{}, logger, 0); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("interval 0 did not return immediately")
	}
	if buf.Len() != 0 {
		t.Errorf("disabled check logged:\n%s", buf.String())
	}
}

// TestIntegrityCheckCleanPassNoOutput: a healthy store logs nothing.
func TestIntegrityCheckCleanPassNoOutput(t *testing.T) {
	st := newMemStore()
	file := []byte("intact weights")
	sum := sha256.Sum256(file)
	key := fmt.Sprintf("pub/%s/%s/org/name/w.bin", goodSHA[:2], goodSHA)
	if err := st.Put(context.Background(), key, bytes.NewReader(file), int64(len(file))); err != nil {
		t.Fatal(err)
	}
	putManifest(t, st, goodSHA, map[string]string{"w.bin": hex.EncodeToString(sum[:])})

	var buf syncBuffer
	logger := log.New(&buf, "", 0)
	if err := checkOneManifest(context.Background(), st, logger, fmt.Sprintf("pub/%s/%s/org/name/@manifest", goodSHA[:2], goodSHA)); err != nil {
		t.Fatalf("checkOneManifest: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("healthy store produced log output:\n%s", buf.String())
	}
}

// TestIntegrityCheckJoinsOnCancel: the loop must exit promptly after ctx
// cancellation (main's WaitGroup join must not hang shutdown), even while a
// pass is between ticks.
func TestIntegrityCheckJoinsOnCancel(t *testing.T) {
	st := newMemStore()
	var buf syncBuffer
	logger := log.New(&buf, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runIntegrityChecks(ctx, st, fixedKeys{[]string{fmt.Sprintf("pub/%s/%s/org/name/@manifest", goodSHA[:2], goodSHA)}}, logger, 20*time.Millisecond)
		close(done)
	}()
	// Let at least one tick+pass run, then cancel.
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("integrity loop did not exit after cancellation (shutdown would hang)")
	}
}

// TestLogMiddlewareRedactsNothingSensitiveAndCounts: the access log line
// carries method/path/status/duration/cache/repo@commit and never an
// Authorization value; bytes_served advances by the body length.
func TestLogMiddlewareRedactsNothingSensitiveAndCounts(t *testing.T) {
	c := &metrics.Counters{}
	var buf syncBuffer
	logger := log.New(&buf, "", 0)
	h := logMiddleware(c, logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Cache", "MISS")
		w.Header().Set("X-Repo-Commit", goodSHA)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello world"))
	}))

	req, _ := http.NewRequest(http.MethodGet, "/org/name/resolve/main/w.bin", nil)
	req.Header.Set("Authorization", "Bearer sekrit-token-value")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	line := buf.String()
	for _, want := range []string{"GET", "/org/name/resolve/main/w.bin", "200", "MISS", goodSHA, "11"} {
		if !strings.Contains(line, want) {
			t.Errorf("access log line %q missing %q", line, want)
		}
	}
	if strings.Contains(line, "sekrit-token-value") || strings.Contains(line, "Bearer") {
		t.Errorf("access log leaked credentials: %q", line)
	}
	if !strings.Contains(c.Render(), "hf_cache_bytes_served_total 11") {
		t.Errorf("bytes_served not counted: %s", c.Render())
	}
	if !strings.Contains(c.Render(), `hf_cache_requests_total{route="file"} 1`) {
		t.Errorf("request not counted under route=file: %s", c.Render())
	}
}

// TestLoadConfigIntegrityInterval covers the new env var.
func TestLoadConfigIntegrityInterval(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "http://localhost:8333")
	t.Setenv("S3_BUCKET", "b")
	t.Setenv("S3_ACCESS_KEY", "k")
	t.Setenv("S3_SECRET_KEY", "s")

	t.Setenv("INTEGRITY_CHECK_INTERVAL", "")
	cfg, err := LoadConfig()
	if err != nil || cfg.IntegrityCheckInterval != time.Hour {
		t.Errorf("default: cfg=%v err=%v, want 1h", cfg.IntegrityCheckInterval, err)
	}

	t.Setenv("INTEGRITY_CHECK_INTERVAL", "0")
	cfg, err = LoadConfig()
	if err != nil || cfg.IntegrityCheckInterval != 0 {
		t.Errorf("0: cfg=%v err=%v, want disabled", cfg.IntegrityCheckInterval, err)
	}

	t.Setenv("INTEGRITY_CHECK_INTERVAL", "250ms")
	cfg, err = LoadConfig()
	if err != nil || cfg.IntegrityCheckInterval != 250*time.Millisecond {
		t.Errorf("250ms: cfg=%v err=%v", cfg.IntegrityCheckInterval, err)
	}

	t.Setenv("INTEGRITY_CHECK_INTERVAL", "-1s")
	if _, err = LoadConfig(); err == nil {
		t.Error("negative interval accepted")
	}

	t.Setenv("INTEGRITY_CHECK_INTERVAL", "bogus")
	if _, err = LoadConfig(); err == nil {
		t.Error("non-duration accepted")
	}
}

// TestMetricszRouteInIndex: the route index lists /metricsz.
func TestMetricszRouteInIndex(t *testing.T) {
	srv := httptest.NewServer(newTestMux())
	t.Cleanup(srv.Close)
	_, body := httpGetBody(t, srv.URL+"/")
	if !strings.Contains(body, "/metricsz") {
		t.Errorf("index does not advertise /metricsz: %s", body)
	}
}

// ---- helpers ----

// fixedKeys is a manifestSource stub for tests.
type fixedKeys struct{ keys []string }

func (f fixedKeys) ManifestKeys() []string { return f.keys }

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func httpGetBody(t *testing.T, rawURL string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(body)
}

// pushOne PUTs one artifact file through the push lane, returning its sha256.
func pushOne(t *testing.T, base, repo, version, file string, body []byte) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut,
		base+"/v1/artifacts/"+repo+"/"+version+"/"+file, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer sekrit")
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("push %s: status %d (%s)", file, resp.StatusCode, raw)
	}
	var out struct {
		SHA256 string `json:"sha256"`
	}
	_ = json.Unmarshal(raw, &out)
	return out.SHA256
}
