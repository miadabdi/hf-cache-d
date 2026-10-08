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

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/proxy"
)

// ---- fixtures ----

// pstore is the in-memory store fake (memStore's shape, local to this
// package so push tests stay self-contained).
type pstore struct {
	mu          sync.Mutex
	objs        map[string][]byte
	failPutKeys map[string]bool
}

func newPStore() *pstore { return &pstore{objs: map[string][]byte{}} }

func (m *pstore) Get(_ context.Context, key string) (io.ReadCloser, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, 0, errNotFound
	}
	return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
}

func (m *pstore) GetRange(_ context.Context, key string, start, end int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, errNotFound
	}
	return io.NopCloser(bytes.NewReader(b[start:end])), nil
}

func (m *pstore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	m.mu.Lock()
	failing := m.failPutKeys[key]
	m.mu.Unlock()
	if failing {
		return fmt.Errorf("pstore: simulated put failure")
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
	b, ok := m.objs[key]
	return ok, int64(len(b)), nil
}

type notFoundErr struct{}

func (notFoundErr) Error() string { return "not found" }

var errNotFound = notFoundErr{}

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
	if resp, _ := l.seal(t, "org/priv", "v1", big, nil, "tok"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("oversized manifest: status %d, want 400", resp.StatusCode)
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
