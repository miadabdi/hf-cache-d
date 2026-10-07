//go:build s3compose

// End-to-end test of the snapshot pull sequence against the real routing
// table (newMux) and the compose SeaweedFS store: metadata → 3 files →
// repeat; pass 2 must make zero upstream file requests and serve identical
// bytes, and the durable manifest must carry the right sha256s.

package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/proxy"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// fakeHub is the same HF stand-in used by the proxy package tests, reduced
// to what the e2e sequence needs: revision resolution and resolve→302→CDN.
type fakeHub struct {
	mu       sync.Mutex
	repo     string
	sha      string // commit served for "main"
	files    map[string][]byte
	cdnHits  int
	upstream string
}

func (f *fakeHub) cdnCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cdnHits
}

func newFakeHub(t *testing.T, repo string, files map[string][]byte) *fakeHub {
	t.Helper()
	// Unique commit sha per run: test packages run in parallel against one
	// shared bucket and the manifest key carries only the sha, so parallel
	// fixtures must not share one (a real commit sha identifies one repo).
	uniq := sha256.Sum256([]byte(repo))
	sha := hex.EncodeToString(uniq[:20]) // 40-hex, the commit shape
	f := &fakeHub{repo: repo, sha: sha, files: files}
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.cdnHits++
		rest := strings.TrimPrefix(r.URL.Path, "/"+repo+"/resolve/")
		_, file, _ := strings.Cut(rest, "/")
		body := f.files[file]
		f.mu.Unlock()
		if body == nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.Write(body)
	}))
	t.Cleanup(cdn.Close)

	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if p, ok := strings.CutPrefix(r.URL.Path, "/api/models/"+repo+"/revision/"); ok {
			s := f.sha
			if p != "main" {
				s = p
			}
			fmt.Fprintf(w, `{"sha":%q,"siblings":[]}`, s)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/"+repo+"/resolve/") {
			w.Header().Set("Location", cdn.URL+r.URL.Path)
			w.WriteHeader(http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(hub.Close)
	f.upstream = hub.URL
	return f
}

// s3Endpoint: compose endpoint, SEAWEEDFS_S3_PORT aware.
func s3Endpoint() string {
	if e := os.Getenv("S3_TEST_ENDPOINT"); e != "" {
		return e
	}
	if p := os.Getenv("SEAWEEDFS_S3_PORT"); p != "" {
		return "http://localhost:" + p
	}
	return "http://localhost:8333"
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// TestSnapshotSequenceE2E: the full metadata→files→repeat flow.
func TestSnapshotSequenceE2E(t *testing.T) {
	repo := fmt.Sprintf("org/e2e-%d", time.Now().UnixNano())
	big := bytes.Repeat([]byte("e2e-big-"), 1<<20) // 8 MiB
	small := []byte("tiny")
	nested := []byte("nested file body")
	files := map[string][]byte{
		"big.bin":              big,
		"small.bin":            small,
		"nested/deep/file.txt": nested,
	}
	f := newFakeHub(t, repo, files)

	st, err := store.New(s3Endpoint(), envOr("S3_BUCKET", "test-bucket"), envOr("S3_ACCESS_KEY", "test"), envOr("S3_SECRET_KEY", "test12345678"))
	if err != nil {
		t.Fatal(err)
	}
	// Probe-and-skip, same contract as the store/proxy suites: unreachable
	// endpoint or foreign SeaweedFS on the port skips with instructions.
	hostport := strings.TrimPrefix(strings.TrimPrefix(s3Endpoint(), "http://"), "https://")
	if conn, err := net.DialTimeout("tcp", hostport, 2*time.Second); err != nil {
		t.Skipf("S3 endpoint %s not reachable (docker compose up -d && ./scripts/dev-s3.sh): %v", s3Endpoint(), err)
	} else {
		conn.Close()
	}
	if _, _, err := st.Get(context.Background(), "auth-probe"); err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Skipf("S3 endpoint %s reachable but not usable with fixture creds: %v", s3Endpoint(), err)
	}

	// The routing table mirrors cmd/hf-cache-d's newMux exactly (metadata
	// lane + file lane on a child mux under "/"); cmd packages are not
	// importable, so the shape is duplicated here deliberately. The mux
	// wiring itself is covered by cmd/hf-cache-d's handler tests.
	mux := http.NewServeMux()
	p := proxy.New(f.upstream, st)
	p.Register(mux)
	fileMux := http.NewServeMux()
	p.RegisterFiles(fileMux)
	mux.Handle("/", fileMux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	paths := []string{"big.bin", "small.bin", "nested/deep/file.txt"}
	sums := map[string]string{}
	for name, body := range files {
		s := sha256.Sum256(body)
		sums[name] = hex.EncodeToString(s[:])
	}

	get := func(file string) (*http.Response, []byte) {
		resp, err := http.Get(srv.URL + "/" + repo + "/resolve/main/" + file)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp, body
	}

	for pass := 1; pass <= 2; pass++ {
		// Metadata first, like a real pull.
		info, _ := http.Get(srv.URL + "/api/models/" + repo)
		info.Body.Close()
		if info.StatusCode != 200 {
			t.Fatalf("pass %d: info status %d", pass, info.StatusCode)
		}

		for _, file := range paths {
			resp, body := get(file)
			want := files[file]
			if resp.StatusCode != 200 {
				t.Fatalf("pass %d %s: status %d", pass, file, resp.StatusCode)
			}
			if !bytes.Equal(body, want) {
				t.Fatalf("pass %d %s: body %d bytes, want %d", pass, file, len(body), len(want))
			}
			wantCache := "MISS"
			if pass == 2 {
				wantCache = "HIT"
			}
			if got := resp.Header.Get("X-Cache"); got != wantCache {
				t.Errorf("pass %d %s: X-Cache = %q, want %s", pass, file, got, wantCache)
			}
			if et := resp.Header.Get("ETag"); pass == 2 && et != `"`+sums[file]+`"` {
				t.Errorf("pass 2 %s: ETag = %q, want stored sha256", file, et)
			}
		}
	}

	// Manifest durable and correct.
	rc, _, err := st.Get(context.Background(), fmt.Sprintf("pub/%s/%s/manifest.json", f.sha[:2], f.sha))
	if err != nil {
		t.Fatalf("manifest get: %v", err)
	}
	defer rc.Close()
	var m struct {
		Identity string            `json:"identity"`
		Files    map[string]string `json:"files"`
	}
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Identity != "hf:"+repo+"@"+f.sha {
		t.Errorf("manifest identity = %q", m.Identity)
	}
	for _, file := range paths {
		if m.Files[file] != sums[file] {
			t.Errorf("manifest Files[%s] = %q, want %q", file, m.Files[file], sums[file])
		}
	}

	// Pass 2 made zero upstream FILE requests (metadata ref hit its TTL).
	if n := f.cdnCount(); n != len(paths) {
		t.Errorf("upstream CDN file requests total = %d, want %d (one per file, pass 1 only)", n, len(paths))
	}
}
