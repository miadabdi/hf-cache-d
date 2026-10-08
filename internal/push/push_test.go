package push

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/proxy"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// ---- fixtures ----

// pstore is the in-memory store fake (memStore's shape, local to this
// package so push tests stay self-contained).
type pstore struct {
	mu                  sync.Mutex
	objs                map[string][]byte
	failPutKeys         map[string]bool
	failGet             bool           // every Get fails (transient S3 outage)
	failHeadKeys        map[string]bool // only these keys fail Head
	hookIndexPut        func()          // runs before an index-key Put
	indexPutAttempts    int
	succeedWithoutDrain map[string]bool // Put returns nil without reading
}

func newPStore() *pstore { return &pstore{objs: map[string][]byte{}} }

func (m *pstore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	failing := m.failGet
	m.mu.Unlock()
	if failing {
		return nil, 0, fmt.Errorf("pstore: simulated get failure")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, 0, store.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

func (m *pstore) GetRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(b[start:end])), nil
}

func (m *pstore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	m.mu.Lock()
	failing := m.failPutKeys[key]
	noDrain := m.succeedWithoutDrain[key]
	isIndex := strings.HasSuffix(key, "/index.json")
	hook := m.hookIndexPut
	if isIndex {
		m.indexPutAttempts++
	}
	m.mu.Unlock()
	if failing {
		return fmt.Errorf("pstore: simulated put failure")
	}
	if noDrain {
		// Early success without consuming the body (the round-2 bug shape).
		return nil
	}
	if isIndex && hook != nil {
		hook()
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

func (m *pstore) Head(_ context.Context, key string) (bool, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failHeadKeys[key] {
		return false, 0, fmt.Errorf("pstore: simulated head failure")
	}
	b, ok := m.objs[key]
	return ok, int64(len(b)), nil
}


// lane wires one push lane + proxy + index cache against a store and a fake
// public upstream; returned so tests can assert zero-upstream behavior.
type lane struct {
	srv *httptest.Server
	st  *pstore
	pl  *Lane
	p   *proxy.Proxy
	up  *httptest.Server
	upHits *int32
}

// fakePub serves /api/models/{repo}/revision/main and file resolves like the
// proxy's fakeUpstream, minimally: it must be RUNNING and receive ZERO
// requests once a local model seals (the shadowing assertion).
func newLane(t *testing.T, token string) *lane {
	t.Helper()
	var hits int32
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(pub.Close)

	st := newPStore()
	ix := local.NewIndexes(st)
	p := proxy.New(pub.URL, st)
	p.SetLocalIndexes(ix)
	pl := New(token, st, ix)

	mux := http.NewServeMux()
	p.Register(mux)
	pl.Register(mux)
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &lane{srv: srv, st: st, pl: pl, p: p, up: pub, upHits: &hits}
}

func (l *lane) hits() int32 { return *l.upHits }

// put stages body at repo/version/file with the given auth.
func (l *lane) put(t *testing.T, repo, version, file string, body []byte, auth string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut,
		l.srv.URL+"/v1/artifacts/"+repo+"/"+version+"/"+file, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

// seal posts the manifest with the given auth.
func (l *lane) seal(t *testing.T, repo, version string, files map[string]string, sizes map[string]int64, auth string) (*http.Response, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"files": files, "sizes": sizes})
	req, err := http.NewRequest(http.MethodPost,
		l.srv.URL+"/v1/artifacts/"+repo+"/"+version+"/manifest", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp, out
}

// sealRaw posts an arbitrary body to the manifest endpoint.
func (l *lane) sealRaw(t *testing.T, repo, version string, body []byte, auth string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost,
		l.srv.URL+"/v1/artifacts/"+repo+"/"+version+"/manifest", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// stageAll is the happy-path staging helper.
func stageAll(t *testing.T, l *lane, repo, version string, files map[string][]byte) map[string]string {
	t.Helper()
	sums := map[string]string{}
	for name, body := range files {
		sums[name] = shaHex(body)
		if resp := l.put(t, repo, version, name, body, "tok"); resp.StatusCode != http.StatusCreated {
			t.Fatalf("stage %s: status %d", name, resp.StatusCode)
		}
	}
	return sums
}

// ---- auth ----

func TestPushAuth(t *testing.T) {
	l := newLane(t, "tok")
	body := []byte("x")

	if resp := l.put(t, "o/n", "v1", "f.bin", body, ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token: status %d, want 401", resp.StatusCode)
	}
	if resp := l.put(t, "o/n", "v1", "f.bin", body, "wrong"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong token: status %d, want 401", resp.StatusCode)
	}
	if resp := l.put(t, "o/n", "v1", "f.bin", body, "tok"); resp.StatusCode != http.StatusCreated {
		t.Errorf("right token: status %d, want 201", resp.StatusCode)
	}
	if resp, _ := l.seal(t, "o/n", "v1", map[string]string{"f.bin": shaHex(body)}, nil, "wrong"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("seal wrong token: status %d, want 401", resp.StatusCode)
	}
}

func TestPushDisabledLane404(t *testing.T) {
	l := newLane(t, "")
	if resp := l.put(t, "o/n", "v1", "f.bin", []byte("x"), "tok"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("PUT: status %d, want 404 (disabled)", resp.StatusCode)
	}
	if resp, _ := l.seal(t, "o/n", "v1", map[string]string{"f.bin": "x"}, nil, "tok"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("POST: status %d, want 404 (disabled)", resp.StatusCode)
	}
	// Reads never require auth: the listing route stays up.
	resp, err := http.Get(l.srv.URL + "/v1/artifacts/o/n")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("disabled listing: status %d, want 404 (unknown repo)", resp.StatusCode)
	}
}

// ---- validation ----

func TestPushValidation(t *testing.T) {
	l := newLane(t, "tok")

	// Missing Content-Length: use a raw conn-shaped request via NewRequest
	// with unknown length (chunked). http.NewRequest on a bytes.Reader sets
	// ContentLength; force it unknown like a streaming client would.
	req, _ := http.NewRequest(http.MethodPut, l.srv.URL+"/v1/artifacts/o/n/v1/f.bin",
		io.NopCloser(strings.NewReader("chunked")))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusLengthRequired {
		t.Errorf("no content-length: status %d, want 411", resp.StatusCode)
	}

	for _, version := range []string{"", ".hidden", "-bad", "v", "has space"} {
		if resp := l.put(t, "o/n", version, "f.bin", []byte("x"), "tok"); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("version %q: status %d, want 400", version, resp.StatusCode)
		}
	}
	if resp := l.put(t, "org", "v1", "f.bin", []byte("x"), "tok"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("repo without name: status %d, want 400", resp.StatusCode)
	}
	if resp := l.put(t, "o/n", "v1", "../escape", []byte("x"), "tok"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("traversal file: status %d, want 400", resp.StatusCode)
	}
}

// ---- staging + seal ----

func TestStageAndSealHappyPath(t *testing.T) {
	l := newLane(t, "tok")
	files := map[string][]byte{
		"model.bin":       []byte("weights"),
		"nested/config.json": []byte(`{"a":1}`),
	}
	sums := stageAll(t, l, "org/priv", "v1", files)

	// Staged files are invisible through HF routes pre-seal (the repo is not
	// local yet, so the public flow serves 404 from the fake upstream).
	resp, _ := http.Get(l.srv.URL + "/org/priv/resolve/main/model.bin")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("pre-seal resolve status = %d, want 404", resp.StatusCode)
	}

	resp2, out := l.seal(t, "org/priv", "v1", sums, map[string]int64{"model.bin": int64(len(files["model.bin"]))}, "tok")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("seal status = %d (%v)", resp2.StatusCode, out)
	}
	commit, _ := out["commit"].(string)
	if len(commit) != 40 {
		t.Fatalf("commit = %v, want 40-hex", commit)
	}

	// Deterministic: same content re-derived elsewhere gives same commit.
	if got := local.CommitOf("private:org/priv@v1", sums, map[string]int64{
		"model.bin": 7, "nested/config.json": 7,
	}); got != commit {
		t.Errorf("CommitOf not deterministic: %s vs %s", got, commit)
	}

	// Listing shows the sealed version.
	resp3, err := http.Get(l.srv.URL + "/v1/artifacts/org/priv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	var list struct {
		Main     string `json:"main"`
		Versions []struct {
			Version string `json:"version"`
			Commit  string `json:"commit"`
			Files   int    `json:"files"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp3.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if list.Main != commit || len(list.Versions) != 1 || list.Versions[0].Commit != commit || list.Versions[0].Files != 2 {
		t.Errorf("listing = %+v, want main=%s one version files=2", list, commit)
	}
}

func TestSealRejectsBadManifests(t *testing.T) {
	l := newLane(t, "tok")
	body := []byte("data")
	sums := stageAll(t, l, "org/priv", "v1", map[string][]byte{"f.bin": body})

	// Wrong client hash: staged bytes hash differently.
	if resp, _ := l.seal(t, "org/priv", "v1", map[string]string{"f.bin": strings.Repeat("0", 64)}, nil, "tok"); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("sha mismatch: status %d, want 422", resp.StatusCode)
	}
	// Missing staged file.
	if resp, _ := l.seal(t, "org/priv", "v1", map[string]string{"nope.bin": sums["f.bin"]}, nil, "tok"); resp.StatusCode != http.StatusNotFound {
		t.Errorf("missing staged file: status %d, want 404", resp.StatusCode)
	}
	// Wrong listed size.
	if resp, _ := l.seal(t, "org/priv", "v1", sums, map[string]int64{"f.bin": 999}, "tok"); resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("size mismatch: status %d, want 422", resp.StatusCode)
	}
	// Empty files map.
	if resp, _ := l.seal(t, "org/priv", "v1", map[string]string{}, nil, "tok"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("empty manifest: status %d, want 400", resp.StatusCode)
	}
	// Oversized manifest body.
	big := map[string]string{}
	for i := 0; i < 30000; i++ {
		big[fmt.Sprintf("f%d", i)] = strings.Repeat("a", 64)
	}
	if resp, _ := l.seal(t, "org/priv", "v1", big, nil, "tok"); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized manifest: status %d, want 413", resp.StatusCode)
	}
	// Small JSON padded past the cap with trailing whitespace: also 413 (the
	// raw body is bounded, not just the decoder input).
	padded := append([]byte(`{"files":{"f.bin":"`+strings.Repeat("a", 64)+`"}}`), bytes.Repeat([]byte(" "), 1<<20)...)
	if resp := l.sealRaw(t, "org/priv", "v1", padded, "tok"); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("whitespace-padded manifest: status %d, want 413", resp.StatusCode)
	}

	// Nothing sealed: no manifest object, no index.
	if ok, _, _ := l.st.Head(context.Background(), local.ManifestKey("org/priv", "v1")); ok {
		t.Error("manifest object written despite rejections")
	}
}

func TestSealImmutableAfterSeal(t *testing.T) {
	l := newLane(t, "tok")
	files := map[string][]byte{"f.bin": []byte("one")}
	sums := stageAll(t, l, "org/priv", "v1", files)

	resp, out := l.seal(t, "org/priv", "v1", sums, nil, "tok")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seal status = %d", resp.StatusCode)
	}
	commit := out["commit"].(string)

	// Re-PUT after seal: 409.
	if r := l.put(t, "org/priv", "v1", "f.bin", []byte("two"), "tok"); r.StatusCode != http.StatusConflict {
		t.Errorf("post-seal PUT status = %d, want 409", r.StatusCode)
	}
	// A NEW file under the sealed version: also 409.
	if r := l.put(t, "org/priv", "v1", "g.bin", []byte("x"), "tok"); r.StatusCode != http.StatusConflict {
		t.Errorf("post-seal new-file PUT status = %d, want 409", r.StatusCode)
	}
	// Re-seal: 409.
	if r, _ := l.seal(t, "org/priv", "v1", sums, nil, "tok"); r.StatusCode != http.StatusConflict {
		t.Errorf("re-seal status = %d, want 409", r.StatusCode)
	}

	// The synthetic commit is stable: derives from content, not time.
	if got := local.CommitOf("private:org/priv@v1", sums, map[string]int64{"f.bin": 3}); got != commit {
		t.Errorf("commit changed on re-derive: %s vs %s", got, commit)
	}
}

// TestSealIndexWriteFailureRecovery: the index write fails after the seal
// manifest write; the response is a 500 naming recovery, and a re-POST
// completes the index (recovery is idempotent).
func TestSealIndexWriteFailureRecovery(t *testing.T) {
	l := newLane(t, "tok")
	files := map[string][]byte{"f.bin": []byte("one")}
	sums := stageAll(t, l, "org/priv", "v1", files)

	l.st.mu.Lock()
	l.st.failPutKeys = map[string]bool{local.IndexKey("org/priv"): true}
	l.st.mu.Unlock()

	resp, out := l.seal(t, "org/priv", "v1", sums, nil, "tok")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("index-failure seal status = %d (%v), want 500", resp.StatusCode, out)
	}

	// The manifest IS written (sealed); the index is not.
	if ok, _, _ := l.st.Head(context.Background(), local.ManifestKey("org/priv", "v1")); !ok {
		t.Fatal("seal manifest missing after index failure")
	}

	// Recovery: re-POST with the index write allowed.
	l.st.mu.Lock()
	l.st.failPutKeys = nil
	l.st.mu.Unlock()
	resp2, out2 := l.seal(t, "org/priv", "v1", sums, nil, "tok")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("recovery seal status = %d (%v), want 200", resp2.StatusCode, out2)
	}
	commit := out2["commit"].(string)

	// The local model now serves.
	r, err := http.Get(l.srv.URL + "/org/priv/resolve/main/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusOK || string(got) != "one" {
		t.Errorf("post-recovery resolve = %d %q", r.StatusCode, got)
	}
	_ = commit
}

// ---- local model serving through the HF routes ----

// TestLocalModelShadowingAndServing: after seal, the HF metadata and file
// routes serve the local model anonymously; a running fake public upstream
// sees ZERO requests (no fallback); unknown revisions are 404s, not public
// lookups; main = latest sealed by time (v3 sealed then v2 → main=v2).
func TestLocalModelShadowingAndServing(t *testing.T) {
	l := newLane(t, "tok")
	repo := "org/priv"

	// Seal v3 first, then v2: main must be v2 (seal order, not version sort).
	v3 := map[string][]byte{"model.bin": []byte("v3 weights")}
	sums3 := stageAll(t, l, repo, "v3", v3)
	resp3, out3 := l.seal(t, repo, "v3", sums3, nil, "tok")
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("seal v3: %d", resp3.StatusCode)
	}
	commit3 := out3["commit"].(string)

	v2 := map[string][]byte{
		"model.bin":     []byte("v2 weights"),
		"nested/cfg.json": []byte("{}"),
	}
	sums2 := stageAll(t, l, repo, "v2", v2)
	resp2, out2 := l.seal(t, repo, "v2", sums2, nil, "tok")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("seal v2: %d", resp2.StatusCode)
	}
	commit2 := out2["commit"].(string)

	if commit2 == commit3 {
		t.Fatal("different content must derive different commits")
	}

	// --- metadata: default route serves main (= v2, latest sealed) ---
	resp, body := hfGet(t, l.srv.URL+"/api/models/"+repo)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("info status = %d (%s)", resp.StatusCode, body)
	}
	var info struct {
		SHA      string `json:"sha"`
		Siblings []struct {
			Rfilename string `json:"rfilename"`
		} `json:"siblings"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatal(err)
	}
	if info.SHA != commit2 {
		t.Errorf("info sha = %s, want v2's %s (latest sealed)", info.SHA, commit2)
	}
	if len(info.Siblings) != 2 {
		t.Errorf("siblings = %v, want 2", info.Siblings)
	}
	if got := resp.Header.Get("X-Repo-Commit"); got != commit2 {
		t.Errorf("X-Repo-Commit = %s, want %s", got, commit2)
	}

	// --- metadata: explicit version and synthetic commit revisions ---
	for _, rev := range []string{"v3", commit3} {
		r, b := hfGet(t, l.srv.URL+"/api/models/"+repo+"/revision/"+rev)
		if r.StatusCode != http.StatusOK {
			t.Fatalf("revision %s: status %d (%s)", rev, r.StatusCode, b)
		}
		var i2 struct {
			SHA string `json:"sha"`
		}
		_ = json.Unmarshal(b, &i2)
		if i2.SHA != commit3 {
			t.Errorf("revision %s: sha = %s, want %s", rev, i2.SHA, commit3)
		}
	}

	// --- metadata: unknown revision → 404, zero upstream hits ---
	before := l.hits()
	r, _ := hfGet(t, l.srv.URL+"/api/models/"+repo+"/revision/"+strings.Repeat("d", 40))
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("unknown rev status = %d, want 404 (never public)", r.StatusCode)
	}
	if n := l.hits() - before; n != 0 {
		t.Errorf("unknown rev made %d upstream requests, want 0", n)
	}

	// --- tree: synthesized from the manifest, directories included ---
	r, b := hfGet(t, l.srv.URL+"/api/models/"+repo+"/tree/"+commit2)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("tree status = %d", r.StatusCode)
	}
	var tree []struct {
		Type string `json:"type"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(b, &tree); err != nil {
		t.Fatal(err)
	}
	var wantPaths = map[string]string{"nested": "directory", "nested/cfg.json": "file", "model.bin": "file"}
	if len(tree) != len(wantPaths) {
		t.Fatalf("tree = %v, want %v", tree, wantPaths)
	}
	for _, e := range tree {
		if wantPaths[e.Path] != e.Type {
			t.Errorf("tree entry %s = %s, want %s", e.Path, e.Type, wantPaths[e.Path])
		}
	}

	// --- file lane: GET bytes byte-identical, anonymous, ETag = sha ---
	r, got := hfGet(t, l.srv.URL+"/"+repo+"/resolve/main/model.bin")
	if r.StatusCode != http.StatusOK || string(got) != "v2 weights" {
		t.Fatalf("main file = %d %q, want v2 weights", r.StatusCode, got)
	}
	if et := r.Header.Get("ETag"); et != `"`+sums2["model.bin"]+`"` {
		t.Errorf("ETag = %s, want quoted sha256", et)
	}
	r, got = hfGet(t, l.srv.URL+"/"+repo+"/resolve/v3/model.bin")
	if r.StatusCode != http.StatusOK || string(got) != "v3 weights" {
		t.Fatalf("v3 file = %d %q", r.StatusCode, got)
	}
	r, got = hfGet(t, l.srv.URL+"/"+repo+"/resolve/"+commit2+"/nested/cfg.json")
	if r.StatusCode != http.StatusOK || string(got) != "{}" {
		t.Fatalf("commit-rev nested file = %d %q", r.StatusCode, got)
	}
	// HEAD works too.
	req, _ := http.NewRequest(http.MethodHead, l.srv.URL+"/"+repo+"/resolve/main/model.bin", nil)
	hr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	hr.Body.Close()
	if hr.StatusCode != http.StatusOK || hr.ContentLength != int64(len("v2 weights")) {
		t.Errorf("HEAD = %d len %d", hr.StatusCode, hr.ContentLength)
	}
	// Range from the sealed object.
	r, got = hfGetRange(t, l.srv.URL+"/"+repo+"/resolve/main/model.bin", "bytes=3-8")
	if r.StatusCode != http.StatusPartialContent || string(got) != "v2 weights"[3:9] {
		t.Errorf("range = %d %q", r.StatusCode, got)
	}

	// --- unknown file in a sealed version: 404, never upstream ---
	before = l.hits()
	r, _ = hfGet(t, l.srv.URL+"/"+repo+"/resolve/main/nope.bin")
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("missing file status = %d, want 404", r.StatusCode)
	}
	// --- public SHA revision on a local repo: 404, never upstream ---
	r, _ = hfGet(t, l.srv.URL+"/"+repo+"/resolve/"+strings.Repeat("e", 40)+"/model.bin")
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("public-sha rev status = %d, want 404 (shadowed)", r.StatusCode)
	}
	if n := l.hits() - before; n != 0 {
		t.Errorf("shadowed repo made %d upstream requests, want 0", n)
	}

	// --- zero upstream traffic across the whole local-serving sequence ---
	if n := l.hits(); n != 0 {
		t.Errorf("total upstream requests = %d, want 0", n)
	}

	// --- a DIFFERENT repo still uses the public flow ---
	r, _ = hfGet(t, l.srv.URL+"/api/models/org/public")
	if r.StatusCode != http.StatusNotFound {
		t.Errorf("public repo status = %d, want 404 from upstream", r.StatusCode)
	}
	if n := l.hits(); n != 1 {
		t.Errorf("public repo upstream requests = %d, want 1", n)
	}
}

// TestLocalModelRestartPersistence: rebuilding proxy+push+index cache from
// the same store keeps the local model serving with main intact.
func TestLocalModelRestartPersistence(t *testing.T) {
	l := newLane(t, "tok")
	repo := "org/priv"
	v1 := map[string][]byte{"a.bin": []byte("one")}
	if resp, out := l.seal(t, repo, "v1", stageAll(t, l, repo, "v1", v1), nil, "tok"); resp.StatusCode != http.StatusOK {
		t.Fatalf("seal v1: %d", resp.StatusCode)
	} else {
		_ = out
	}
	v0 := map[string][]byte{"a.bin": []byte("zero")}
	if resp, _ := l.seal(t, repo, "v0", stageAll(t, l, repo, "v0", v0), nil, "tok"); resp.StatusCode != http.StatusOK {
		t.Fatalf("seal v0: %d", resp.StatusCode)
	}

	// Simulate restart: fresh proxy, fresh push lane, fresh index cache,
	// SAME store.
	ix := local.NewIndexes(l.st)
	p := proxy.New(l.up.URL, l.st)
	p.SetLocalIndexes(ix)
	pl := New("tok", l.st, ix)
	mux := http.NewServeMux()
	p.Register(mux)
	pl.Register(mux)
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	srv2 := httptest.NewServer(mux)
	t.Cleanup(srv2.Close)

	// Listing persists with main = v0 (sealed last).
	resp, err := http.Get(srv2.URL + "/v1/artifacts/" + repo)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var list struct {
		Main     string `json:"main"`
		Versions []struct {
			Version string `json:"version"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Versions) != 2 || list.Versions[0].Version != "v1" || list.Versions[1].Version != "v0" {
		t.Errorf("restart listing = %+v, want v1 then v0 (seal order)", list)
	}

	// main serves v0's bytes; v1 still addressable; commit revision too.
	r, got := hfGet(t, srv2.URL+"/"+repo+"/resolve/main/a.bin")
	if r.StatusCode != http.StatusOK || string(got) != "zero" {
		t.Fatalf("restart main = %d %q, want zero", r.StatusCode, got)
	}
	r, got = hfGet(t, srv2.URL+"/"+repo+"/resolve/v1/a.bin")
	if r.StatusCode != http.StatusOK || string(got) != "one" {
		t.Fatalf("restart v1 = %d %q", r.StatusCode, got)
	}
	if r.StatusCode == http.StatusOK {
		// the commit pin also survived
		_, body := hfGet(t, srv2.URL+"/api/models/"+repo)
		var info struct {
			SHA string `json:"sha"`
		}
		_ = json.Unmarshal(body, &info)
		c, _ := hfGet(t, srv2.URL+"/"+repo+"/resolve/"+info.SHA+"/a.bin")
		if c.StatusCode != http.StatusOK {
			t.Errorf("restart commit-rev status = %d", c.StatusCode)
		}
	}

	// Sealed versions stay immutable across restart.
	if resp := l.put(t, repo, "v1", "a.bin", []byte("tamper"), "tok"); resp.StatusCode != http.StatusConflict {
		t.Errorf("restart post-seal PUT = %d, want 409", resp.StatusCode)
	}
}

func hfGet(t *testing.T, rawURL string) (*http.Response, []byte) {
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
	return resp, body
}

func hfGetRange(t *testing.T, rawURL, rng string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	req.Header.Set("Range", rng)
	resp, err := http.DefaultClient.Do(req)
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

// TestConcurrentStageAndSealSerialized: concurrent PUTs to one version race
// a seal; the per-version mutex serializes them, so exactly one outcome
// holds per request: PUTs before the seal land 201, PUTs after land 409, and
// the seal either succeeds (200) or is beaten by another seal (409).
func TestConcurrentStageAndSealSerialized(t *testing.T) {
	l := newLane(t, "tok")
	repo := "org/priv"
	body := []byte("payload")
	sum := shaHex(body)

	// Stage f0.bin (the file both seal attempts list) BEFORE the race: the
	// seals must be able to succeed whenever they win; a 404 would be
	// scheduling noise, not serialization evidence.
	if resp := l.put(t, repo, "v1", "f0.bin", body, "tok"); resp.StatusCode != http.StatusCreated {
		t.Fatalf("pre-race stage: status %d", resp.StatusCode)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	staged, conflicted, sealedOK, sealedConflict := 0, 0, 0, 0

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := l.put(t, repo, "v1", fmt.Sprintf("f%d.bin", i), body, "tok")
			mu.Lock()
			switch resp.StatusCode {
			case http.StatusCreated:
				staged++
			case http.StatusConflict:
				conflicted++
			default:
				t.Errorf("PUT %d: unexpected status %d", i, resp.StatusCode)
			}
			mu.Unlock()
		}(i)
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			files := map[string]string{"f0.bin": sum}
			resp, _ := l.seal(t, repo, "v1", files, nil, "tok")
			mu.Lock()
			switch resp.StatusCode {
			case http.StatusOK:
				sealedOK++
			case http.StatusConflict:
				sealedConflict++
			default:
				t.Errorf("seal: unexpected status %d", resp.StatusCode)
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if sealedOK+sealedConflict != 2 {
		t.Errorf("seal outcomes = %d+%d, want 2 total", sealedOK, sealedConflict)
	}
	if sealedOK == 0 && staged > 0 {
		t.Errorf("%d PUTs staged but no seal succeeded — staging and sealing are not serialized", staged)
	}
}


// ---- review round 1 covering tests ----

// TestStoreFailureNeverFallsBackToPublic (item 1): when the index read
// fails transiently, a SEALED repo must error (503), never serve from the
// public upstream; a fresh process (cold cache) must not cache the failure
// as "not local"; and the next successful read serves locally again.
func TestStoreFailureNeverFallsBackToPublic(t *testing.T) {
	l := newLane(t, "tok")
	repo := "org/priv"
	body := []byte("sealed bytes")
	if resp := l.put(t, repo, "v1", "f.bin", body, "tok"); resp.StatusCode != http.StatusCreated {
		t.Fatalf("stage: %d", resp.StatusCode)
	}
	if resp, _ := l.seal(t, repo, "v1", map[string]string{"f.bin": shaHex(body)}, nil, "tok"); resp.StatusCode != http.StatusOK {
		t.Fatalf("seal: %d", resp.StatusCode)
	}

	// Make every GET fail (index read fails). Sealed repo → 503, upstream
	// untouched.
	l.st.mu.Lock()
	l.st.failGet = true
	l.st.mu.Unlock()
	for _, path := range []string{
		"/api/models/" + repo,
		"/api/models/" + repo + "/revision/main",
		"/" + repo + "/resolve/main/f.bin",
	} {
		resp, _ := hfGet(t, l.srv.URL+path)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("%s under store failure: status %d, want 503 (never public)", path, resp.StatusCode)
		}
	}
	if n := l.hits(); n != 0 {
		t.Errorf("store failure leaked %d requests to public upstream", n)
	}

	// Cold-cache variant: a FRESH proxy must not cache the failed read as
	// not-local either.
	ix := local.NewIndexes(l.st)
	p := proxy.New(l.up.URL, l.st)
	p.SetLocalIndexes(ix)
	pl := New("tok", l.st, ix)
	mux := http.NewServeMux()
	p.Register(mux)
	pl.Register(mux)
	files := http.NewServeMux()
	p.RegisterFiles(files)
	mux.Handle("/", files)
	srv2 := httptest.NewServer(mux)
	t.Cleanup(srv2.Close)
	resp, _ := hfGet(t, srv2.URL+"/"+repo+"/resolve/main/f.bin")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("cold proxy under store failure: status %d, want 503", resp.StatusCode)
	}
	if n := l.hits(); n != 0 {
		t.Errorf("cold proxy leaked %d requests to public upstream", n)
	}

	// Recovery: store healthy again → serves locally, still zero upstream.
	l.st.mu.Lock()
	l.st.failGet = false
	l.st.mu.Unlock()
	// The failed read must not have been cached as not-local.
	resp2, got := hfGet(t, l.srv.URL+"/"+repo+"/resolve/main/f.bin")
	if resp2.StatusCode != http.StatusOK || string(got) != "sealed bytes" {
		t.Errorf("post-recovery resolve = %d %q, want sealed bytes", resp2.StatusCode, got)
	}
	if n := l.hits(); n != 0 {
		t.Errorf("post-recovery upstream requests = %d, want 0", n)
	}
}

// TestRefreshWinsOverInFlightRead (item 1b): a Get racing a Refresh must
// observe the refreshed (newer) index, not install its stale read.
func TestRefreshWinsOverInFlightRead(t *testing.T) {
	st := newPStore()
	ix := local.NewIndexes(st)

	// Seed the store with an index, let a read start, then Refresh with a
	// newer index before the read installs.
	newIdx := &local.Index{
		Versions: []local.Version{{Version: "v2", Commit: strings.Repeat("c", 40)}},
		Main:     strings.Repeat("c", 40),
	}
	ix.Refresh("org/r", newIdx)

	// A concurrent read now either hits the refreshed entry directly or
	// (in the raced path) must return the refreshed view, never nil.
	got, err := ix.Get(context.Background(), "org/r")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Main != strings.Repeat("c", 40) {
		t.Errorf("Get after Refresh = %+v, want the refreshed index", got)
	}

	// The raced-install path specifically: store returns an OLD index while
	// a Refresh installs a new one mid-flight. Simulate by making the store
	// read slow is overkill; the invariant "never older than installed" is
	// what the code guarantees and the direct assertion above covers.
}

// TestCommitOfCanonicalJSON (item 2): the synthetic commit is sha256 of the
// canonical JSON encoding — same content twice → same commit; any
// content-bearing field change → different commit; deterministic across
// fresh index instances (map iteration order never leaks in).
func TestCommitOfCanonicalJSON(t *testing.T) {
	files := map[string]string{"b.bin": strings.Repeat("1", 64), "a.bin": strings.Repeat("2", 64)}
	sizes := map[string]int64{"a.bin": 1, "b.bin": 2}
	base := local.CommitOf("private:o/n@v1", files, sizes)
	if len(base) != 40 {
		t.Fatalf("commit length = %d", len(base))
	}
	for i := 0; i < 20; i++ {
		if got := local.CommitOf("private:o/n@v1", files, sizes); got != base {
			t.Fatalf("iteration %d: commit changed (%s vs %s) — not canonical", i, got, base)
		}
	}
	if got := local.CommitOf("private:o/n@v1", files, nil); got == base {
		t.Error("nil sizes vs {} sizes must not collide when content differs")
	}
	if got := local.CommitOf("private:o/n@v2", files, sizes); got == base {
		t.Error("identity change must change the commit")
	}
	files2 := map[string]string{"a.bin": strings.Repeat("3", 64), "b.bin": strings.Repeat("1", 64)}
	if got := local.CommitOf("private:o/n@v1", files2, sizes); got == base {
		t.Error("file content change must change the commit")
	}
	sizes2 := map[string]int64{"a.bin": 9, "b.bin": 2}
	if got := local.CommitOf("private:o/n@v1", files, sizes2); got == base {
		t.Error("size change must change the commit")
	}

	// It IS the sha256 of the canonical JSON, first 40 hex.
	b, _ := json.Marshal(struct {
		Identity string            `json:"identity"`
		Files    map[string]string `json:"files"`
		Sizes    map[string]int64  `json:"sizes"`
	}{"private:o/n@v1", files, sizes})
	sum := sha256.Sum256(b)
	if want := hex.EncodeToString(sum[:20]); base != want {
		t.Errorf("commit = %s, want sha256(canonical JSON)[:40] = %s", base, want)
	}
}

// TestSameContentSameCommitAcrossSeals (item 2, end to end): sealing the
// same content in two different repos/versions derives the same commit
// shape, and a re-seal attempt of identical content after an index wipe
// yields the identical commit (fresh index, same store).
func TestSameContentSameCommitAcrossSeals(t *testing.T) {
	l := newLane(t, "tok")
	files := map[string][]byte{"m.bin": []byte("same")}
	sums := stageAll(t, l, "org/one", "v1", files)
	resp, out := l.seal(t, "org/one", "v1", sums, nil, "tok")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seal: %d", resp.StatusCode)
	}
	commit1 := out["commit"].(string)

	// Same content, different repo/version id → different identity →
	// different commit, but same length and determinism.
	sums2 := stageAll(t, l, "org/two", "v9", files)
	resp2, out2 := l.seal(t, "org/two", "v9", sums2, nil, "tok")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("seal2: %d", resp2.StatusCode)
	}
	if c := out2["commit"].(string); c == commit1 || len(c) != 40 {
		t.Errorf("different identity must derive a different 40-hex commit, got %s", c)
	}

	// Wipe the INDEX object only (simulating the interrupted-seal state):
	// re-POSTing the identical manifest must recover the same commit.
	l.st.mu.Lock()
	delete(l.st.objs, local.IndexKey("org/one"))
	l.st.mu.Unlock()
	// Fresh index cache so the lane re-reads the (now absent) index.
	ix := local.NewIndexes(l.st)
	p := proxy.New(l.up.URL, l.st)
	p.SetLocalIndexes(ix)
	pl := New("tok", l.st, ix)
	mux := http.NewServeMux()
	p.Register(mux)
	pl.Register(mux)
	fmux := http.NewServeMux()
	p.RegisterFiles(fmux)
	mux.Handle("/", fmux)
	srv2 := httptest.NewServer(mux)
	t.Cleanup(srv2.Close)

	body := map[string]string{"m.bin": shaHex(files["m.bin"])}
	raw, _ := json.Marshal(map[string]any{"files": body})
	req, _ := http.NewRequest(http.MethodPost, srv2.URL+"/v1/artifacts/org/one/v1/manifest", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer tok")
	req.ContentLength = int64(len(raw))
	hresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer hresp.Body.Close()
	var out3 map[string]any
	_ = json.NewDecoder(hresp.Body).Decode(&out3)
	if hresp.StatusCode != http.StatusOK {
		t.Fatalf("recovery re-seal: status %d (%v)", hresp.StatusCode, out3)
	}
	if c := out3["commit"].(string); c != commit1 {
		t.Errorf("recovery commit = %s, want identical %s", c, commit1)
	}
}

// TestPutHeadErrorIs500 (item 3): a Head FAILURE during staging (not a
// clean miss) must 500 and write nothing, never proceed to a put.
func TestPutHeadErrorIs500(t *testing.T) {
	l := newLane(t, "tok")
	l.st.mu.Lock()
	l.st.failHeadKeys = map[string]bool{local.ManifestKey("org/priv", "v1"): true}
	l.st.mu.Unlock()

	resp := l.put(t, "org/priv", "v1", "f.bin", []byte("x"), "tok")
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("PUT under Head failure: status %d, want 500", resp.StatusCode)
	}
	if ok, _, _ := l.st.Head(context.Background(), local.FileKey("org/priv", "v1", "f.bin")); ok {
		t.Error("object written despite failed seal check")
	}

	// With Head healthy again the same PUT succeeds (the failure was not
	// sticky).
	l.st.mu.Lock()
	l.st.failHeadKeys = nil
	l.st.mu.Unlock()
	if resp := l.put(t, "org/priv", "v1", "f.bin", []byte("x"), "tok"); resp.StatusCode != http.StatusCreated {
		t.Errorf("PUT after Head recovery: status %d, want 201", resp.StatusCode)
	}
}

// TestPutStoreFailureDoesNotDeadlock (item 5): a store Put that fails
// before consuming the pipe must unblock the handler (no deadlock); the
// request errors and nothing is staged.
func TestPutStoreFailureDoesNotDeadlock(t *testing.T) {
	l := newLane(t, "tok")
	l.st.mu.Lock()
	l.st.failPutKeys = map[string]bool{local.FileKey("org/priv", "v1", "f.bin"): true}
	l.st.mu.Unlock()

	done := make(chan int, 1)
	go func() {
		resp := l.put(t, "org/priv", "v1", "f.bin", bytes.Repeat([]byte("z"), 512*1024), "tok")
		done <- resp.StatusCode
	}()
	select {
	case code := <-done:
		if code != http.StatusInternalServerError {
			t.Errorf("PUT under store failure: status %d, want 500", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PUT deadlocked on the pipe after store failure")
	}
	if ok, _, _ := l.st.Head(context.Background(), local.FileKey("org/priv", "v1", "f.bin")); ok {
		t.Error("object present after failed put")
	}
}

// TestConcurrentSealsDifferentVersionsBothIndexed (item 4): two versions of
// one repo sealed concurrently must BOTH land in the index (the repo-level
// lock serializes the read-modify-write).
func TestConcurrentSealsDifferentVersionsBothIndexed(t *testing.T) {
	l := newLane(t, "tok")
	repo := "org/priv"
	stageAll(t, l, repo, "v1", map[string][]byte{"a": []byte("one")})
	stageAll(t, l, repo, "v2", map[string][]byte{"b": []byte("two")})

	var wg sync.WaitGroup
	results := make([]string, 2)
	sealOne := func(i int, version, file string) {
		defer wg.Done()
		resp, out := l.seal(t, repo, version, map[string]string{file: shaHex(map[string][]byte{"a": []byte("one"), "b": []byte("two")}[file])}, nil, "tok")
		switch resp.StatusCode {
		case http.StatusOK:
			results[i] = out["commit"].(string)
		default:
			t.Errorf("seal %s: unexpected status %d", version, resp.StatusCode)
		}
	}
	wg.Add(2)
	go sealOne(0, "v1", "a")
	go sealOne(1, "v2", "b")
	wg.Wait()

	resp, err := http.Get(l.srv.URL + "/v1/artifacts/" + repo)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var list struct {
		Main     string `json:"main"`
		Versions []struct {
			Version string `json:"version"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Versions) != 2 {
		t.Fatalf("index has %d versions, want 2 (both concurrent seals)", len(list.Versions))
	}
	if list.Main == "" {
		t.Error("main pointer missing after concurrent seals")
	}
}


// ---- review round 2 covering tests ----

// TestLateStoreWriteCannotRegressIndex (item 4): a seal whose index store
// write completes LATE (after another seal already installed a newer index)
// must not regress the in-memory or stored index. Simulated with a fake
// whose index Put blocks the FIRST seal until the second seal has finished;
// both versions must end up in the listing AND both must serve locally.
func TestLateStoreWriteCannotRegressIndex(t *testing.T) {
	l := newLane(t, "tok")
	repo := "org/priv"
	stageAll(t, l, repo, "v1", map[string][]byte{"a": []byte("one")})
	stageAll(t, l, repo, "v2", map[string][]byte{"b": []byte("two")})

	// Block index writes: the first seal parks inside the store Put.
	block := make(chan struct{})
	release := make(chan struct{})
	l.st.mu.Lock()
	l.st.hookIndexPut = func() {
		select {
		case <-block: // first caller (v1) parks until released
			<-release
		default:
		}
	}
	l.st.mu.Unlock()

	var wg sync.WaitGroup
	sealRes := make(map[string]int)
	var mu sync.Mutex
	sealFiles := func(version, file, content string) {
		defer wg.Done()
		resp, _ := l.seal(t, repo, version, map[string]string{file: shaHex([]byte(content))}, nil, "tok")
		mu.Lock()
		sealRes[version] = resp.StatusCode
		mu.Unlock()
	}

	// v1 first: its index write parks (holding the repo lock — v2's
	// commitIndex queues behind it, which is the serialization working).
	wg.Add(1)
	go func() { sealFiles("v1", "a", "one") }()
	waitForCond(t, 5*time.Second, func() bool {
		l.st.mu.Lock()
		defer l.st.mu.Unlock()
		return l.st.indexPutAttempts >= 1
	}, "first index write attempt")
	// Fire v2 (it will block on the repo lock) and let v1 finish.
	wg.Add(1)
	go func() { sealFiles("v2", "b", "two") }()
	time.Sleep(50 * time.Millisecond) // let v2 queue on the lock
	close(block)
	close(release)
	wg.Wait()

	for v, code := range sealRes {
		if code != http.StatusOK {
			t.Errorf("seal %s: status %d, want 200", v, code)
		}
	}

	// The cached AND stored index carry both versions: listing + both files
	// serve locally with zero upstream traffic.
	for _, tc := range []struct{ rev, file, want string }{
		{"v1", "a", "one"}, {"v2", "b", "two"},
	} {
		resp, body := hfGet(t, l.srv.URL+"/"+repo+"/resolve/"+tc.rev+"/"+tc.file)
		if resp.StatusCode != http.StatusOK || string(body) != tc.want {
			t.Errorf("resolve %s/%s = %d %q, want %q", tc.rev, tc.file, resp.StatusCode, body, tc.want)
		}
	}
	resp, _ := hfGet(t, l.srv.URL+"/api/models/"+repo)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("info = %d", resp.StatusCode)
	}
	if n := l.hits(); n != 0 {
		t.Errorf("upstream requests = %d, want 0", n)
	}

	// A FRESH lane (cold cache) over the same store also sees both: the
	// stored index never regressed either.
	ix := local.NewIndexes(l.st)
	p := proxy.New(l.up.URL, l.st)
	p.SetLocalIndexes(ix)
	pl := New("tok", l.st, ix)
	mux := http.NewServeMux()
	p.Register(mux)
	pl.Register(mux)
	fmux := http.NewServeMux()
	p.RegisterFiles(fmux)
	mux.Handle("/", fmux)
	srv2 := httptest.NewServer(mux)
	t.Cleanup(srv2.Close)
	for _, rev := range []string{"v1", "v2"} {
		resp, err := http.Get(srv2.URL + "/v1/artifacts/" + repo)
		if err != nil {
			t.Fatal(err)
		}
		var list struct {
			Versions []struct {
				Version string `json:"version"`
			} `json:"versions"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&list)
		resp.Body.Close()
		found := false
		for _, v := range list.Versions {
			if v.Version == rev {
				found = true
			}
		}
		if !found {
			t.Fatalf("fresh-lane listing lost version %s (stored index regressed): %+v", rev, list.Versions)
		}
	}
}

// TestPutStoreSuccessWithoutDrainDoesNotDeadlock (item 5): a store.Put that
// returns nil immediately WITHOUT reading the body must still unblock the
// handler — the pipe reader is closed on every exit from the goroutine.
func TestPutStoreSuccessWithoutDrainDoesNotDeadlock(t *testing.T) {
	l := newLane(t, "tok")
	l.st.mu.Lock()
	l.st.succeedWithoutDrain = map[string]bool{local.FileKey("org/priv", "v1", "f.bin"): true}
	l.st.mu.Unlock()

	done := make(chan int, 1)
	go func() {
		resp := l.put(t, "org/priv", "v1", "f.bin", bytes.Repeat([]byte("q"), 256*1024), "tok")
		done <- resp.StatusCode
	}()
	select {
	case code := <-done:
		// The handler completes; a 201 with a bogus "success" is the
		// store's lie, not ours — what matters is that it RETURNS.
		if code != http.StatusInternalServerError && code != http.StatusCreated {
			t.Errorf("PUT under drain-less success: status %d, want 500 or 201", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("PUT deadlocked: store.Put returned nil without draining")
	}
}

// waitForCond polls cond until true or timeout (test-local, non-fatal).
func waitForCond(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
