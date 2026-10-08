//go:build s3compose || client

// Shared fixtures for the compose-backed suites (e2e and the real-client
// suite): the fake HF upstream, the probe-and-skip compose store, and the
// push-lane server wiring.

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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/proxy"
	"github.com/miadabdi/hf-cache-d/internal/push"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// fakeHub is the HF stand-in used by the compose-backed suites. It serves
// what stock clients need: revision resolution (sha + non-empty siblings, so
// snapshot_download skips its list_repo_tree fallback) and
// resolve→302→CDN with ETag/X-Linked-* headers.
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
		sum := sha256.Sum256(body)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		h := w.Header()
		h.Set("ETag", etag)
		h.Set("X-Linked-ETag", etag) // what HF's CDN sends; stock clients favor it
		h.Set("X-Linked-Size", fmt.Sprint(len(body)))
		h.Set("Content-Length", fmt.Sprint(len(body)))
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
			names := make([]string, 0, len(f.files))
			for name := range f.files {
				names = append(names, name)
			}
			sort.Strings(names)
			siblings := make([]map[string]string, 0, len(names))
			for _, name := range names {
				siblings = append(siblings, map[string]string{"rfilename": name})
			}
			out, _ := json.Marshal(map[string]any{"id": repo, "sha": s, "siblings": siblings})
			w.Write(out)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/"+repo+"/resolve/") {
			// Like real HF: the pre-redirect hop stamps X-Linked-ETag
			// (sha256) and X-Linked-Size, so a HEAD that stops here — our
			// headClient — still gets full metadata.
			rest := strings.TrimPrefix(r.URL.Path, "/"+repo+"/resolve/")
			if _, file, ok := strings.Cut(rest, "/"); ok {
				if body := f.files[file]; body != nil {
					sum := sha256.Sum256(body)
					w.Header().Set("X-Linked-ETag", `"`+hex.EncodeToString(sum[:])+`"`)
					w.Header().Set("X-Linked-Size", fmt.Sprint(len(body)))
				}
			}
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

// newComposeStore builds the compose store with the suite-wide probe-and-skip
// contract: unreachable endpoint or foreign SeaweedFS on the port skips with
// instructions.
func newComposeStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(s3Endpoint(), envOr("S3_BUCKET", "test-bucket"), envOr("S3_ACCESS_KEY", "test"), envOr("S3_SECRET_KEY", "test12345678"))
	if err != nil {
		t.Fatal(err)
	}
	hostport := strings.TrimPrefix(strings.TrimPrefix(s3Endpoint(), "http://"), "https://")
	if conn, err := net.DialTimeout("tcp", hostport, 2*time.Second); err != nil {
		msg := fmt.Sprintf("S3 endpoint %s not reachable (docker compose up -d && ./scripts/dev-s3.sh): %v", s3Endpoint(), err)
		if os.Getenv("S3_TEST_STRICT") == "1" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	} else {
		conn.Close()
	}
	if _, _, err := st.Get(context.Background(), "auth-probe"); err != nil && !errors.Is(err, store.ErrNotFound) {
		msg := fmt.Sprintf("S3 endpoint %s reachable but not usable with fixture creds: %v", s3Endpoint(), err)
		if os.Getenv("S3_TEST_STRICT") == "1" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
	return st
}

// pushLane wires a push lane + proxy + local index cache against st and the
// given upstream, mirroring newMux's shape (compose tests cannot import cmd).
// An empty token disables the push lane (public-pull-only fixture).
func pushLane(t *testing.T, st *store.Store, upstream string, token string) *httptest.Server {
	t.Helper()
	ix := local.NewIndexes(st)
	p := proxy.New(upstream, st)
	p.SetLocalIndexes(ix)
	pl := push.New(token, st, ix)
	mux := http.NewServeMux()
	p.Register(mux)
	pl.Register(mux)
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// mustPut stages a file through the authenticated push lane.
func mustPut(t *testing.T, base, repo, version, file string, body []byte) string {
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
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("push put %s: status %d", file, resp.StatusCode)
	}
	var out struct {
		SHA256 string `json:"sha256"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out.SHA256
}

// mustSeal posts the seal manifest and returns the synthetic commit.
func mustSeal(t *testing.T, base, repo, version string, files map[string]string, sizes map[string]int64) string {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"files": files, "sizes": sizes})
	req, err := http.NewRequest(http.MethodPost,
		base+"/v1/artifacts/"+repo+"/"+version+"/manifest", bytes.NewReader(body))
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
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("seal: status %d (%s)", resp.StatusCode, raw)
	}
	var out struct {
		Commit string `json:"commit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Commit
}

// barrier waits until every file of repo@sha is manifest-published and its
// object headed: a detached publish (object Put + manifest RMW) can still be
// in flight when the responses returned, so zero-upstream / HIT assertions
// need it — otherwise correct code fails the test.
func barrier(t *testing.T, st *store.Store, sha, repo string, files map[string][]byte) {
	t.Helper()
	sums := map[string]string{}
	for name, body := range files {
		s := sha256.Sum256(body)
		sums[name] = hex.EncodeToString(s[:])
	}
	deadline := time.Now().Add(30 * time.Second)
	for name, sum := range sums {
		ok := false
		for time.Now().Before(deadline) {
			rc, _, err := st.Get(context.Background(), fmt.Sprintf("pub/%s/%s/%s/@manifest", sha[:2], sha, repo))
			if err == nil {
				var m struct {
					Files map[string]string `json:"files"`
				}
				if json.NewDecoder(rc).Decode(&m) == nil && m.Files[name] == sum {
					rc.Close()
					ok = true
					break
				}
			}
			if rc != nil {
				rc.Close()
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !ok {
			t.Fatalf("barrier: %s never published to the manifest", name)
		}
		exists, _, err := st.Head(context.Background(), fmt.Sprintf("pub/%s/%s/%s/%s", sha[:2], sha, repo, name))
		if err != nil || !exists {
			t.Fatalf("barrier: object for %s missing (err %v)", name, err)
		}
	}
}
