package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
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
}

func newFakeUpstream(t *testing.T) (*httptest.Server, *fakeUpstream) {
	t.Helper()
	f := &fakeUpstream{hits: map[string]int{}, repo: "org/name", main: sha1}
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
			fmt.Fprintf(w, `{"sha":%q,"siblings":[{"rfilename":"f-%s"}]}`, sha, sha[:8])
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
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv, f
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
	mu   sync.Mutex
	objs map[string][]byte
}

func newMemStore() *memStore { return &memStore{objs: map[string][]byte{}} }

func (m *memStore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, 0, store.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
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

func (m *memStore) has(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objs[key]
	return ok
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
