package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/store"
)

const (
	sha1 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sha2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// fakeUpstream is a minimal Hugging Face Hub stand-in: it serves revision
// bodies, tree pages with a Link header, records request counts, and can be
// switched to error modes mid-test. The repo defaults to org/name; the
// compose integration test overrides it so each run starts cache-cold
// against the shared bucket.
//
// ── REAL-HF FIDELITY CHECKLIST ──────────────────────────────────────────
// Wire shapes real HF sends that this fake MUST reproduce. Two verification
// rounds were lost to fakes simpler than reality in exactly one detail.
// When extending this fake, check each line; when a verification round
// finds a new divergence, ADD IT HERE before fixing the code:
//
//	[ ] query params: hf_hub 0.36.x sends ?blobs=True (NOT files_metadata)
//	[ ] non-LFS 307 hop: X-Linked-ETag = git sha1, NO X-Linked-Size,
//	    Content-Length = redirect-message length, Location RELATIVE
//	    (/api/resolve-cache/…) — true size one hop later
//	[ ] LFS 302 hop: X-Linked-ETag = file sha256 + X-Linked-Size
//	[ ] xet/LFS CDN hop: own ETag is a CAS hash (NOT the file sha256)
//	[ ] unknown repo (anonymous request): 401, not 404
//	[ ] default branch is "main"; bad revisions 404 while repo exists
type fakeUpstream struct {
	mu        sync.Mutex
	hits      map[string]int
	repo      string // repo path under /api/models/, default "org/name"
	main      string // sha currently served for rev "main"
	status    int    // non-zero: every reply gets this status
	badSHA    bool   // serve a malformed sha in revision bodies
	link      string // Link header attached to cursor-less tree replies
	gotAuth   string
	gotCookie string

	// File-lane state (Task 3).
	files    map[string][]byte // repo-relative path -> body served for repo@main
	lfs      map[string]bool   // repo-relative path -> served via 302 with X-Linked-* (default true when nil)
	truncate int               // >0: serve only the first N bytes then hang up
	truncateOnce int           // >0: cut the FIRST full GET at N bytes once (resume tests), then serve fully
	slow     int               // >0: CDN ms sleep per 8KiB chunk (disconnect tests)
	xetMode  bool              // CDN hop carries only its CAS ETag (no X-Linked-ETag), like xet-backed LFS
	cdnSeen  map[string]int    // fake-CDN request counts by path (not query)
	cdn      *httptest.Server
}

func newFakeUpstream(t *testing.T) (*httptest.Server, *fakeUpstream) {
	t.Helper()
	f := &fakeUpstream{hits: map[string]int{}, repo: "org/name", main: sha1, files: map[string][]byte{}, lfs: map[string]bool{}, cdnSeen: map[string]int{}}
	f.cdn = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.cdnSeen[r.URL.Path]++
		// Path shape: /{repo}/resolve/{rev}/{file...}
		rest, _ := strings.CutPrefix(strings.TrimPrefix(r.URL.Path, "/"+f.repo+"/"), "resolve/")
		_, fname, _ := strings.Cut(rest, "/")
		file, truncate, slow, xet := f.files[fname], f.truncate, f.slow, f.xetMode
		if f.truncateOnce > 0 && r.Header.Get("Range") == "" {
			truncate = f.truncateOnce
			f.truncateOnce = 0 // one-shot: only the FIRST full GET is cut
		}
		f.mu.Unlock()
		if file == nil {
			http.NotFound(w, r)
			return
		}
		etag := fmt.Sprintf(`"%s"`, fakeETag(file))
		if xet {
			// xet-backed LFS: the CDN hop knows only its own CAS hash —
			// deliberately NOT the file sha256 — and no X-Linked-* headers.
			w.Header().Set("ETag", `"cas-`+fakeETag(file)+`"`)
		} else {
			w.Header().Set("X-Linked-ETag", etag)
			w.Header().Set("X-Linked-Size", fmt.Sprint(len(file)))
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		if r.Method == http.MethodHead {
			w.WriteHeader(200)
			return
		}
		if rg := r.Header.Get("Range"); rg != "" {
			start, end, ok := parseTestRange(rg, len(file))
			if !ok {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(file)))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(file)))
			w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
			// Real HF CDN ranged answers carry the CAS ETag for xet files;
			// the sha256 truth is only on the pre-redirect hop. Relaying
			// this ETag would flip against warm HITs (same class as the
			// full-GET cold-HEAD flip).
			if xet {
				w.Header().Set("ETag", `"cas-`+fakeETag(file)+`"`)
			}
			w.WriteHeader(http.StatusPartialContent)
			w.Write(file[start : end+1])
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(file)))
		w.WriteHeader(200)
		if slow > 0 {
			// Drip the body so a cold GET stays in flight long enough for a
			// client to disconnect mid-download.
			for off := 0; off < len(file); off += 8192 {
				end := off + 8192
				if end > len(file) {
					end = len(file)
				}
				w.Write(file[off:end])
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				time.Sleep(time.Duration(slow) * time.Millisecond)
			}
			return
		}
		if truncate > 0 && truncate < len(file) {
			w.Write(file[:truncate])
			// Hang up mid-body: connection closed before Content-Length is
			// satisfied, exactly like an upstream truncation.
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					conn.Close()
				}
			}
			return
		}
		w.Write(file)
	}))
	t.Cleanup(f.cdn.Close)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.hits[r.URL.Path+"?"+r.URL.RawQuery]++
		main, status, badSHA, link, repo := f.main, f.status, f.badSHA, f.link, f.repo
		f.gotAuth = r.Header.Get("Authorization")
		f.gotCookie = r.Header.Get("Cookie")
		f.mu.Unlock()

		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if rev, ok := strings.CutPrefix(r.URL.Path, "/api/models/"+repo+"/revision/"); ok {
			sha := main
			if rev != "main" {
				sha = rev
			}
			if badSHA {
				sha = "not-a-sha"
			}
			if len(sha) >= 8 {
				fmt.Fprintf(w, `{"sha":%q,"siblings":[{"rfilename":"f-%s"}]}`, sha, sha[:8])
			} else {
				// Short garbage revision (e.g. v99bad): HF 404s these — the
				// repo exists, the revision does not.
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/models/"+repo+"/tree/") {
			if r.URL.Query().Get("cursor") == "" && link != "" {
				w.Header().Set("Link", `<`+link+`>; rel="next"`)
				w.Write([]byte(`[{"type":"file","path":"page1.bin"}]`))
				return
			}
			w.Write([]byte(`[{"type":"file","path":"page2.bin"}]`))
			return
		}
		// File lane: /{repo}/resolve/{rev}/{file} → redirect to the fake CDN,
		// mirroring how HF hands files to its CDN. The hub server never
		// serves file bodies itself. LFS files (default) get a 302 whose
		// hop carries X-Linked-ETag (sha256) + X-Linked-Size — what a
		// no-follow HEAD needs. NON-LFS files get HF's real behavior: a 307
		// whose hop carries X-Linked-ETag (git sha1) but NO X-Linked-Size,
		// whose Content-Length is the redirect message body itself (NOT the
		// file size), and whose Location is RELATIVE (/api/resolve-cache/…)
		// — Go's client rejects relative URLs unless resolved first.
		if _, ok := strings.CutPrefix(r.URL.Path, "/"+repo+"/resolve/"); ok {
			if f.files == nil || len(f.files) == 0 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			rest := strings.TrimPrefix(r.URL.Path, "/"+repo+"/resolve/")
			fname, _, _ := strings.Cut(strings.TrimPrefix(rest, main+"/"), "/")
			body, isLFS := f.files[fname], f.lfs[fname]
			if body != nil {
				if isLFS {
					sum := sha256.Sum256(body)
					w.Header().Set("X-Linked-ETag", `"`+hex.EncodeToString(sum[:])+`"`)
					w.Header().Set("X-Linked-Size", fmt.Sprint(len(body)))
					w.Header().Set("Location", f.cdn.URL+r.URL.Path)
				} else {
					w.Header().Set("X-Linked-ETag", `"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"`) // 40-hex git sha1 shape
					w.Header().Set("Location", "/api/resolve-cache"+r.URL.Path)                   // RELATIVE, like real HF
				}
			} else {
				w.Header().Set("Location", f.cdn.URL+r.URL.Path)
			}
			msg := []byte("Temporary Redirect. The document has moved.")
			w.Header().Set("Content-Length", fmt.Sprint(len(msg))) // the redirect message's own length, like real HF 307s
			w.WriteHeader(http.StatusTemporaryRedirect)
			w.Write(msg)
			return
		}
		// resolve-cache: HF's same-host relative redirect target for non-LFS
		// files. HEADs here carry the TRUE Content-Length (and an ETag that
		// is the git sha1, not the file sha256). f.mu is held by the outer
		// handler — read the map under it, no re-locking.
		if p, ok := strings.CutPrefix(r.URL.Path, "/api/resolve-cache/"); ok {
			fname := ""
			if i := strings.LastIndex(p, "/"); i >= 0 {
				fname = p[i+1:]
			}
			body := f.files[fname]
			if body != nil {
				w.Header().Set("Content-Length", fmt.Sprint(len(body)))
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Header().Set("ETag", `"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"`)
				w.WriteHeader(http.StatusOK)
				if r.Method != http.MethodHead {
					w.Write(body)
				}
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Metadata: unknown repos get HF's real answer for anonymous
		// requests — 401, which stock clients surface as
		// RepositoryNotFoundError via the 401→repo-404 mapping.
		if strings.HasPrefix(r.URL.Path, "/api/models/") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv, f
}

// fakeETag is the deterministic etag the fake CDN stamps on a body: the
// first 32 hex chars of its sha256, quoted like HF's X-Linked-ETag.
func fakeETag(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])[:32]
}

// parseTestRange parses a single "bytes=a-b" or open-ended "bytes=a-"
// range against a body length (real HF supports open-ended resumes; the
// fake must too or resume tests are unfalsifiable).
func parseTestRange(rg string, size int) (start, end int, ok bool) {
	rg, found := strings.CutPrefix(rg, "bytes=")
	if !found || strings.Contains(rg, ",") {
		return 0, 0, false
	}
	a, b, found := strings.Cut(rg, "-")
	if !found {
		return 0, 0, false
	}
	if a == "" {
		return 0, 0, false // suffix ranges unused in tests
	}
	start, err := strconv.Atoi(a)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if b == "" {
		end = size - 1 // open-ended: through EOF
	} else if end, err = strconv.Atoi(b); err != nil || end < start {
		return 0, 0, false
	}
	if start >= size {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

func (f *fakeUpstream) count(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[key]
}

func (f *fakeUpstream) lastAuth() (auth, cookie string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotAuth, f.gotCookie
}

// setRepo switches the repo the fake serves (compose test isolation).
func (f *fakeUpstream) setRepo(repo string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.repo = repo
}

func (f *fakeUpstream) repoOf() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.repo
}

// memStore is the in-memory storeAPI fake used by offline tests.
type memStore struct {
	mu          sync.Mutex
	objs        map[string][]byte
	failPut     bool            // every Put returns an error (S3 failure simulation)
	failPutKeys map[string]bool // only these keys fail (targeted failure)
}

func newMemStore() *memStore { return &memStore{objs: map[string][]byte{}} }

func (m *memStore) setFailPut(keys map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failPutKeys = keys
}

func (m *memStore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, 0, store.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

func (m *memStore) GetRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	if start < 0 || end <= start || end > int64(len(b)) {
		return nil, fmt.Errorf("invalid range [%d,%d) for %d bytes", start, end, len(b))
	}
	return io.NopCloser(bytes.NewReader(b[start:end])), nil
}

func (m *memStore) Head(_ context.Context, key string) (bool, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	return ok, int64(len(b)), nil
}

func (m *memStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	if m.failNow(key) {
		return errors.New("memstore: simulated put failure")
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = b
	return nil
}

func (m *memStore) failNow(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failPut || m.failPutKeys[key]
}

func (m *memStore) has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objs[key]
	return ok
}

// delete removes an object, simulating S3 data loss / retention sweep.
func (m *memStore) delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
}

func (m *memStore) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ks := make([]string, 0, len(m.objs))
	for k := range m.objs {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// fakeClock drives Proxy.now for TTL tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time      { return c.t }
func (c *fakeClock) Add(d time.Duration) { c.t = c.t.Add(d) }

func newTestProxy(t *testing.T, upstream string, st *memStore) (*Proxy, *fakeClock) {
	t.Helper()
	p := New(upstream, st)
	fc := &fakeClock{t: time.Now()}
	p.now = fc.Now
	return p, fc
}

func newTestServer(p *Proxy) *httptest.Server {
	mux := http.NewServeMux()
	p.Register(mux)
	// File lane on a child mux under "/": see RegisterFiles for why it
	// cannot share the main mux with /api/models/.
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	return httptest.NewServer(mux)
}

func proxyGet(t *testing.T, rawURL string, hdrs map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func TestInfoDefaultBranchResolveAndCache(t *testing.T) {
	up, f := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, body1 := proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Cache"); got != "MISS" {
		t.Errorf("X-Cache = %q, want MISS", got)
	}
	if got := resp.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("X-Repo-Commit = %q, want %s", got, sha1)
	}
	if !strings.Contains(string(body1), `"sha":"`+sha1+`"`) {
		t.Errorf("body = %s, want sha %s", body1, sha1)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Errorf("resolve calls = %d, want 1", n)
	}
	if n := f.count("/api/models/org/name/revision/" + sha1 + "?"); n != 1 {
		t.Errorf("pinned fetch calls = %d, want 1", n)
	}
	wantKey := "pub/aa/" + sha1 + "/api/models/org/name/info.json"
	if !st.has(wantKey) {
		t.Errorf("store missing %s; have %v", wantKey, st.keys())
	}

	// Warm request: served from cache, byte-identical, no upstream traffic.
	resp2, body2 := proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("warm status = %d, want 200", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Cache"); got != "HIT" {
		t.Errorf("warm X-Cache = %q, want HIT", got)
	}
	if got := resp2.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("warm X-Repo-Commit = %q, want %s", got, sha1)
	}
	if !bytes.Equal(body1, body2) {
		t.Error("cached body differs from first response")
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Errorf("resolve calls after warm = %d, want 1", n)
	}
	if n := f.count("/api/models/org/name/revision/" + sha1 + "?"); n != 1 {
		t.Errorf("pinned fetch calls after warm = %d, want 1", n)
	}
}

func TestRefTTLExpiryMainMovedOldPinnedImmutable(t *testing.T) {
	up, f := newFakeUpstream(t)
	st := newMemStore()
	p, fc := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, body1 := proxyGet(t, srv.URL+"/api/models/org/name/revision/main", nil)
	if resp.Header.Get("X-Repo-Commit") != sha1 {
		t.Fatalf("X-Repo-Commit = %q, want %s", resp.Header.Get("X-Repo-Commit"), sha1)
	}
	// Within TTL: no refetch.
	proxyGet(t, srv.URL+"/api/models/org/name/revision/main", nil)
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Fatalf("resolve calls within TTL = %d, want 1", n)
	}

	// Past TTL with main moved: re-resolve, new pinned body, old data intact.
	fc.Add(6 * time.Minute)
	f.mu.Lock()
	f.main = sha2
	f.mu.Unlock()

	resp3, body3 := proxyGet(t, srv.URL+"/api/models/org/name/revision/main", nil)
	if resp3.Header.Get("X-Repo-Commit") != sha2 {
		t.Errorf("X-Repo-Commit after move = %q, want %s", resp3.Header.Get("X-Repo-Commit"), sha2)
	}
	if !strings.Contains(string(body3), sha2) {
		t.Errorf("body after move = %s, want sha %s", body3, sha2)
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 2 {
		t.Errorf("resolve calls after TTL = %d, want 2", n)
	}
	if n := f.count("/api/models/org/name/revision/" + sha2 + "?"); n != 1 {
		t.Errorf("pinned fetch of new sha = %d, want 1", n)
	}

	// Old commit's pinned data stays immutable and cached.
	resp4, body4 := proxyGet(t, srv.URL+"/api/models/org/name/revision/"+sha1, nil)
	if resp4.Header.Get("X-Cache") != "HIT" {
		t.Errorf("old sha X-Cache = %q, want HIT", resp4.Header.Get("X-Cache"))
	}
	if !bytes.Equal(body4, body1) {
		t.Error("old pinned body changed")
	}
	if n := f.count("/api/models/org/name/revision/" + sha1 + "?"); n != 1 {
		t.Errorf("old sha pinned fetches = %d, want 1 (no refetch)", n)
	}
}

func TestCommitSHAPinnedSkipsResolve(t *testing.T) {
	up, f := newFakeUpstream(t)
	p, _ := newTestProxy(t, up.URL, newMemStore())
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/revision/"+sha1, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("status=%d X-Cache=%q, want 200/MISS", resp.StatusCode, resp.Header.Get("X-Cache"))
	}
	if got := resp.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("X-Repo-Commit = %q, want %s", got, sha1)
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 0 {
		t.Errorf("resolve calls = %d, want 0 (sha used directly)", n)
	}
	if n := f.count("/api/models/org/name/revision/" + sha1 + "?"); n != 1 {
		t.Errorf("pinned fetch calls = %d, want 1", n)
	}
}

func TestTreePaginationLinkRewrite(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.link = up.URL + "/api/models/org/name/tree/" + sha1 + "?cursor=p2"
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, body := proxyGet(t, srv.URL+"/api/models/org/name/tree/main?recursive=true", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("X-Repo-Commit = %q, want %s", got, sha1)
	}
	if !strings.Contains(string(body), "page1.bin") {
		t.Errorf("body = %s, want page1", body)
	}
	// recursive passthrough reaches upstream.
	if n := f.count("/api/models/org/name/tree/" + sha1 + "?recursive=true"); n != 1 {
		t.Errorf("tree fetch with recursive = %d, want 1", n)
	}

	link := resp.Header.Get("Link")
	if link == "" {
		t.Fatal("missing Link header on page 1")
	}
	inner := link[strings.IndexByte(link, '<')+1 : strings.IndexByte(link, '>')]
	u, err := url.Parse(inner)
	if err != nil {
		t.Fatalf("Link URL %q: %v", inner, err)
	}
	srvURL, _ := url.Parse(srv.URL)
	if u.Scheme != "http" || u.Host != srvURL.Host {
		t.Errorf("Link host = %s://%s, want %s", u.Scheme, u.Host, srvURL.Host)
	}
	if u.Path != "/api/models/org/name/tree/"+sha1 {
		t.Errorf("Link path = %s, want /api/models/org/name/tree/%s", u.Path, sha1)
	}
	if u.Query().Get("cursor") != "p2" {
		t.Errorf("Link cursor = %q, want p2", u.Query().Get("cursor"))
	}

	// Follow the rewritten link: page 2 through the proxy, cached separately.
	resp2, body2 := proxyGet(t, u.String(), nil)
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("page 2 status = %d, want 200", resp2.StatusCode)
	}
	if !strings.Contains(string(body2), "page2.bin") {
		t.Errorf("page 2 body = %s, want page2", body2)
	}
	if resp2.Header.Get("Link") != "" {
		t.Errorf("page 2 Link = %q, want none", resp2.Header.Get("Link"))
	}
	if n := f.count("/api/models/org/name/tree/" + sha1 + "?cursor=p2"); n != 1 {
		t.Errorf("page 2 upstream fetches = %d, want 1", n)
	}

	// Warm page 1 (from cache) must still advertise the next page: the
	// upstream Link travels through the cached envelope, rewritten to us.
	resp3, _ := proxyGet(t, srv.URL+"/api/models/org/name/tree/main?recursive=true", nil)
	if resp3.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("warm page 1 X-Cache = %q, want HIT", resp3.Header.Get("X-Cache"))
	}
	if resp3.Header.Get("Link") == "" {
		t.Error("warm page 1 lost its Link header: clients would see a truncated listing")
	}
	// Both pages came from one single ref resolution.
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Errorf("resolve calls = %d, want 1 (page 2 is sha-pinned)", n)
	}
	base := "pub/aa/" + sha1 + "/api/models/org/name/tree.json"
	if !st.has(base+"?recursive=true") || !st.has(base+"?cursor=p2") {
		t.Errorf("tree cache keys = %v, want both pages cached distinctly", st.keys())
	}
}

func TestUpstream500RetriedThen502(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.status = http.StatusInternalServerError
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, body := proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"error"`) {
		t.Errorf("body = %s, want JSON error", body)
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 2 {
		t.Errorf("attempts = %d, want 2 (1 retry on 5xx)", n)
	}
	if ks := st.keys(); len(ks) != 0 {
		t.Errorf("5xx must not be cached, stored %v", ks)
	}
}

func TestUpstream404Mapped(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.status = http.StatusNotFound
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, body := proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"error"`) {
		t.Errorf("body = %s, want JSON error", body)
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 404)", n)
	}
	if ks := st.keys(); len(ks) != 0 {
		t.Errorf("404 must not be cached, stored %v", ks)
	}
}

func TestMalformedSHANotCached(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.badSHA = true
	f.mu.Unlock()
	p, _ := newTestProxy(t, up.URL, newMemStore())
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/revision/main", nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	// A malformed sha must not poison the ref cache: the next request asks
	// upstream again.
	proxyGet(t, srv.URL+"/api/models/org/name/revision/main", nil)
	if n := f.count("/api/models/org/name/revision/main?"); n != 2 {
		t.Errorf("attempts = %d, want 2 (malformed sha not cached)", n)
	}
}

func TestInvalidShapes(t *testing.T) {
	up, _ := newFakeUpstream(t)
	p, _ := newTestProxy(t, up.URL, newMemStore())
	srv := newTestServer(p)
	defer srv.Close()

	for _, path := range []string{
		"/api/models/solo",
		"/api/models/a/b/c",
		"/api/models/.org/name",
		"/api/models/org..x/name",
		"/api/models/org/name/revision",
		"/api/models/org/name/revision/.dot",
		"/api/models/org/name/tree/",
		"/api/models/o%20rg/name",
	} {
		t.Run(path, func(t *testing.T) {
			resp, body := proxyGet(t, srv.URL+path, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			if !strings.Contains(string(body), `"error"`) {
				t.Errorf("body = %s, want JSON error", body)
			}
		})
	}
}

func TestQueryAllowlistKeysAndFetch(t *testing.T) {
	up, f := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Junk params must not mint new store keys: same canonical key as the
	// bare request, second call is a cache HIT with zero extra upstream
	// traffic.
	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/tree/main?junk="+sha1, nil)
	if resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("junk-param first call X-Cache = %q, want MISS", resp.Header.Get("X-Cache"))
	}
	resp2, _ := proxyGet(t, srv.URL+"/api/models/org/name/tree/main", nil)
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("bare second call X-Cache = %q, want HIT (junk param shares the canonical key)", resp2.Header.Get("X-Cache"))
	}
	if got := len(st.keys()); got != 1 {
		t.Errorf("store keys = %v, want exactly 1", st.keys())
	}
	if n := f.count("/api/models/org/name/tree/" + sha1 + "?"); n != 1 {
		t.Errorf("upstream tree fetches = %d, want 1 (junk not forwarded)", n)
	}

	// Allowlisted variants produce distinct keys...
	for _, q := range []string{"recursive=true", "expand=false", "limit=100", "cursor=xyz"} {
		proxyGet(t, srv.URL+"/api/models/org/name/tree/main?"+q, nil)
	}
	keys := st.keys()
	if len(keys) != 5 {
		t.Errorf("store keys = %v, want 5 (bare + 4 variants)", keys)
	}
	for _, k := range keys[1:] {
		if !strings.Contains(k, "tree.json?") {
			t.Errorf("variant key %q lost its canonical query", k)
		}
	}

	// ...and pass through to upstream (allowlisted, order-independent).
	proxyGet(t, srv.URL+"/api/models/org/name/tree/main?limit=5&recursive=true&junk=1", nil)
	if n := f.count("/api/models/org/name/tree/" + sha1 + "?limit=5&recursive=true"); n != 1 {
		t.Errorf("upstream fetch with allowlisted params = %d, want 1 (got count: %d)", n, f.count("/api/models/org/name/tree/"+sha1+"?limit=5&recursive=true"))
	}
	// Reordered params share the canonical key with the previous fetch.
	resp3, _ := proxyGet(t, srv.URL+"/api/models/org/name/tree/main?recursive=true&limit=5", nil)
	if resp3.Header.Get("X-Cache") != "HIT" {
		t.Errorf("reordered params X-Cache = %q, want HIT (canonical key is sorted)", resp3.Header.Get("X-Cache"))
	}
}

// TestBlobsParamForwarded pins the blobs allowlist: hf_hub 0.36.x sends
// ?blobs=True on the wire for sibling size info (NOT files_metadata — that
// param is never sent by the real client). It must reach upstream so
// try_adopt's size-diff verification works through the mirror.
func TestBlobsParamForwarded(t *testing.T) {
	up, f := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name?blobs=True", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if n := f.count("/api/models/org/name/revision/" + sha1 + "?blobs=True"); n != 1 {
		t.Errorf("pinned fetches with blobs = %d, want 1 (param must be forwarded)", n)
	}
	// Distinct canonical key from the bare request (different body shape).
	proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if got := len(st.keys()); got != 2 {
		t.Errorf("store keys = %v, want 2 (blobs variant + bare)", st.keys())
	}
}

func TestMetadataBodyLimitRejectsOversizedUpstream(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), 32<<20+1))
	}))
	defer up.Close()
	st := newMemStore()
	p := New(up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()
	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/revision/"+sha1, nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("oversized response = %d, want 502", resp.StatusCode)
	}
	if keys := st.keys(); len(keys) != 0 {
		t.Fatalf("oversized metadata cached as %v", keys)
	}
}

func TestOversizedCachedEnvelopeIsCorruptMiss(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	key := fmt.Sprintf("pub/%s/%s/api/models/org/name/tree.json", sha1[:2], sha1)
	oversized := append([]byte(`{"body":"`), bytes.Repeat([]byte("x"), 32<<20+1)...)
	oversized = append(oversized, []byte(`"}`)...)
	if err := st.Put(context.Background(), key, bytes.NewReader(oversized), int64(len(oversized))); err != nil {
		t.Fatal(err)
	}
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()
	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/tree/main", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("oversized cached envelope: status %d cache %q, want 200 MISS", resp.StatusCode, resp.Header.Get("X-Cache"))
	}
}

func TestCursorMultiplicityAndLength(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()
	url := srv.URL + "/api/models/org/name/tree/main?cursor=abc"
	if resp, _ := proxyGet(t, url, nil); resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("single cursor = %q, want MISS", resp.Header.Get("X-Cache"))
	}
	if resp, _ := proxyGet(t, url+"&cursor=xyz", nil); resp.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("duplicate cursor = %q, want same canonical key", resp.Header.Get("X-Cache"))
	}
	if resp, _ := proxyGet(t, url+strings.Repeat("x", 1025), nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("long cursor = %d, want 400", resp.StatusCode)
	}
	if keys := st.keys(); len(keys) != 1 {
		t.Fatalf("invalid cursors minted keys: %v", keys)
	}
}

func TestNon404ClientErrorsNotRetried(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.status = http.StatusTooManyRequests // 429: mapped 502, must NOT retry
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Errorf("attempts = %d, want 1 (4xx other than 404 is not retried)", n)
	}
	if ks := st.keys(); len(ks) != 0 {
		t.Errorf("429 must not be cached, stored %v", ks)
	}
}

func TestNoCredentialsForwarded(t *testing.T) {
	up, f := newFakeUpstream(t)
	p, _ := newTestProxy(t, up.URL, newMemStore())
	srv := newTestServer(p)
	defer srv.Close()

	proxyGet(t, srv.URL+"/api/models/org/name", map[string]string{
		"Authorization": "Bearer sekrit",
		"Cookie":        "session=1",
	})
	auth, cookie := f.lastAuth()
	if auth != "" || cookie != "" {
		t.Errorf("upstream saw Authorization=%q Cookie=%q, both must be empty", auth, cookie)
	}
}

// TestXetReadTokenStub: hub 1.x on xet-backed repos calls
// /api/models/{repo}/xet-read-token/{hash} BEFORE falling back to classic
// resolve; a 400 BadRequestError (unknown path under repo) is opaque. A
// plain 404 is what a non-xet repo answers, so the client falls back
// cleanly without every consumer setting HF_HUB_DISABLE_XET=1.
func TestXetReadTokenStub(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/xet-read-token/abc123", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("xet-read-token status = %d, want 404 (clean classic fallback)", resp.StatusCode)
	}
}

// TestMetadataClientHasOwnTransport: the metadata lane must not share a
// transport with the file lane — saturating file pulls starve small API
// calls otherwise (fleet issue #3: revision GETs 502'd for 2m during big
// transfers).
func TestMetadataClientHasOwnTransport(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	if p.client == p.fileClient {
		t.Fatal("metadata client shares the file client — no lane isolation")
	}
	if p.client.Transport == http.DefaultTransport || p.client.Transport == nil && p.fileClient.Transport == nil {
		t.Log("note: both nil transports would share http.DefaultTransport")
	}
	if p.client.Transport == nil || p.fileClient.Transport == nil {
		t.Fatal("lanes must each own an explicit transport (nil falls back to the SHARED DefaultTransport)")
	}
	if p.client.Transport == p.fileClient.Transport {
		t.Fatal("lanes share one transport — connection pool contention")
	}
}
