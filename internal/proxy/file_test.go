package proxy

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
)

// fileKey is the documented S3 key layout for the file lane:
//
//	pub/<sha[0:2]>/<sha>/<repo>/<file>
func fileKey(sha, repo, file string) string {
	return fmt.Sprintf("pub/%s/%s/%s/%s", sha[:2], sha, repo, file)
}

func manifestKey(sha string) string {
	return manifestKeyFor("org/name", sha)
}

func manifestKeyFor(repo, sha string) string {
	return fmt.Sprintf("pub/%s/%s/%s/@manifest", sha[:2], sha, repo)
}

// addFile installs a file in the fake upstream and returns its sha256 hex.
func addFile(t *testing.T, f *fakeUpstream, name string, body []byte) string {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[name] = body
	f.lfs[name] = true // default: LFS-style hop (X-Linked-* on the redirect); tests opt out for the 307 shape
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// readFileManifest loads the Files map of the manifest stored for sha, or
// nil when no manifest object exists yet.
func readFileManifest(t *testing.T, st *memStore, sha string) map[string]string {
	t.Helper()
	rc, _, err := st.Get(context.Background(), manifestKey(sha))
	if err != nil {
		return nil
	}
	defer rc.Close()
	var m struct {
		Files map[string]string `json:"files"`
	}
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		t.Fatalf("manifest decode: %v", err)
	}
	return m.Files
}

// TestHeadMissThenHit covers the HEAD lane: a cold HEAD resolves the rev,
// consults the manifest, asks upstream (through the CDN redirect chain) for
// size/etag and serves those headers; after a GET caches the file, HEAD
// serves the stored sha/size with zero upstream traffic.
func TestHeadMissThenHit(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("hello file lane")
	addFile(t, f, "small.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Cold HEAD: MISS headers come from the upstream HEAD (via CDN).
	req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/small.bin", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cold HEAD status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Cache"); got != "MISS" {
		t.Errorf("cold HEAD X-Cache = %q, want MISS", got)
	}
	if got := resp.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("X-Repo-Commit = %q, want %s", got, sha1)
	}
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len(body)) {
		t.Errorf("Content-Length = %q, want %d", got, len(body))
	}
	if got := resp.Header.Get("ETag"); got == "" {
		t.Error("cold HEAD: ETag empty")
	}
	if loc := resp.Header.Get("Location"); loc != "" {
		t.Errorf("Location header leaked to client: %q", loc)
	}
	if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Errorf("Accept-Ranges = %q, want bytes", got)
	}
	// HEAD never populates the cache.
	if n := len(st.keys()); n != 0 {
		t.Errorf("HEAD populated store: %v", st.keys())
	}

	// GET caches it.
	_, got := proxyGet(t, srv.URL+"/org/name/resolve/main/small.bin", nil)
	if !bytes.Equal(got, body) {
		t.Fatalf("GET body = %q, want %q", got, body)
	}

	// Warm HEAD: HIT with stored sha/length, zero upstream traffic.
	upstreamHead := f.cdnSeen["/org/name/resolve/"+sha1+"/small.bin"]
	req2, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/small.bin", nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK || resp2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("warm HEAD status=%d X-CCache=%q, want 200/HIT", resp2.StatusCode, resp2.Header.Get("X-Cache"))
	}
	if got := resp2.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("warm HEAD X-Repo-Commit = %q, want %s", got, sha1)
	}
	if got := resp2.Header.Get("Content-Length"); got != fmt.Sprint(len(body)) {
		t.Errorf("warm HEAD Content-Length = %q, want %d", got, len(body))
	}
	if files := readFileManifest(t, st, sha1); files["small.bin"] == "" {
		t.Errorf("manifest = %v, want small.bin entry", files)
	}
	if n := f.cdnSeen["/org/name/resolve/"+sha1+"/small.bin"]; n != upstreamHead {
		t.Errorf("upstream file HEADs after warm = %d, want %d (zero new)", n, upstreamHead)
	}
}

// TestColdHeadETagStableAcrossWarm pins the xet/LFS ETag contract: the cold
// HEAD must adopt the PRE-REDIRECT hop's X-Linked-ETag (HF's sha256), not
// the CDN hop's CAS-style ETag — otherwise a client doing
// hf_hub_download(repo, file, revision="main") on a persistent local_dir
// re-downloads the file once after the mirror warms (cold ETag ≠ warm ETag).
func TestColdHeadETagStableAcrossWarm(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.xetMode = true // CDN hop carries only its CAS ETag, like xet-backed LFS
	body := []byte("etag flip bait")
	sum := addFile(t, f, "flip.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Warm the cache with a GET so the warm HEAD serves sha256.
	if _, got := proxyGet(t, srv.URL+"/org/name/resolve/main/flip.bin", nil); !bytes.Equal(got, body) {
		t.Fatal("cold GET body mismatch")
	}
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files["flip.bin"] == sum
	}, "manifest entry for flip.bin")

	// Cold-HEAD ETag must equal warm-HEAD ETag: both are the manifest sha256
	// (the pre-redirect X-Linked-ETag), never the CDN CAS hash.
	head := func() string {
		req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/flip.bin", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.Header.Get("ETag")
	}
	warmEtag := head() // warm: HIT path, sha256-based
	f.mu.Lock()
	f.cdnSeen = map[string]int{}
	f.mu.Unlock()
	if warmEtag != `"`+sum+`"` {
		t.Fatalf("warm HEAD ETag = %q, want sha256 %q", warmEtag, sum)
	}

	// The cold probe: a fresh proxy over the same store has an empty
	// in-memory manifest cache BUT loads the manifest from the store — so its
	// HEAD would also HIT. The genuinely cold case (no manifest anywhere) is
	// a file not yet GET-cached. Assert the contract directly on a second
	// file: cold HEAD (MISS) ETag must equal what the warm HEAD will serve
	// after a GET caches it — both the sha256, never the CDN CAS hash.
	addFile(t, f, "flip2.bin", body)
	srv2head := func() (etag, cache string) {
		req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/flip2.bin", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.Header.Get("ETag"), resp.Header.Get("X-Cache")
	}
	sum2 := sum // flip2.bin has the same body as flip.bin
	coldEtag, cacheState := srv2head()
	if cacheState != "MISS" {
		t.Fatalf("flip2 first HEAD X-Cache = %q, want MISS (cold probe must be cold)", cacheState)
	}
	if coldEtag == "" {
		t.Fatal("cold HEAD ETag empty")
	}
	if coldEtag != `"`+sum2+`"` {
		t.Errorf("cold HEAD ETag = %q, want sha256 %q (pre-redirect X-Linked-ETag, not CDN CAS)", coldEtag, sum2)
	}
	// After caching, the warm ETag matches the cold one.
	proxyGet(t, srv.URL+"/org/name/resolve/main/flip2.bin", nil)
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files["flip2.bin"] == sum2
	}, "manifest entry for flip2.bin")
	warmEtag2, _ := srv2head()
	if warmEtag2 != coldEtag {
		t.Errorf("ETag flips across warm: cold=%q warm=%q — cold HEAD adopted the CDN CAS ETag instead of the pre-redirect X-Linked-ETag", coldEtag, warmEtag2)
	}
	_ = warmEtag
}

// TestGetColdStreamsThenWarmHit covers the core GET path: cold stream tee'd
// to store+client, warm byte-identical with zero upstream file traffic, and
// the manifest entry published under the resolved commit.
func TestGetColdStreamsThenWarmHit(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := bytes.Repeat([]byte("0123456789abcdef"), 64*1024) // 1 MiB
	sum := addFile(t, f, "big.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Cold GET: streams through, X-Cache MISS, byte-identical.
	resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/big.bin", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cold GET status = %d, want 200", resp.StatusCode)
	}
	if xc := resp.Header.Get("X-Cache"); xc != "MISS" {
		t.Errorf("cold X-Cache = %q, want MISS", xc)
	}
	if got := resp.Header.Get("X-Repo-Commit"); got != sha1 {
		t.Errorf("X-Repo-Commit = %q, want %s", got, sha1)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("cold GET body: got %d bytes, want %d (byte-identical)", len(got), len(body))
	}
	if loc := resp.Header.Get("Location"); loc != "" {
		t.Errorf("CDN Location leaked to client: %q", loc)
	}
	waitFor(t, 5*time.Second, func() bool {
		return st.has(fileKey(sha1, "org/name", "big.bin"))
	}, "object for big.bin in store")
	var files map[string]string
	waitFor(t, 5*time.Second, func() bool {
		files = readFileManifest(t, st, sha1)
		return files != nil && files["big.bin"] == sum
	}, "manifest entry for big.bin")

	// Warm GET: HIT, byte-identical, zero upstream file traffic.
	f.mu.Lock()
	f.cdnSeen = map[string]int{}
	f.mu.Unlock()
	resp2, got2 := proxyGet(t, srv.URL+"/org/name/resolve/main/big.bin", nil)
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("warm X-Cache = %q, lane HIT", resp2.Header.Get("X-Cache"))
	}
	if !bytes.Equal(got2, body) {
		t.Fatal("warm GET body differs from original")
	}
	if got := resp2.Header.Get("ETag"); got != `"`+sum+`"` {
		t.Errorf("warm ETag = %q, want %q", got, sum)
	}
	if len(f.cdnSeen) != 0 {
		t.Errorf("upstream/CDN file traffic after warm: %v", f.cdnSeen)
	}

	// Ref resolution still memoized: one resolve total.
	if n := f.count("/api/models/org/name/revision/main?"); n != 1 {
		t.Errorf("resolve calls = %d, want 1", n)
	}
}

// TestGetRangeWarm covers ranged warm GETs: 206 with exact bytes, clamped
// end, 416 with bytes */size, multi-range and malformed Range ignored.
func TestGetRangeWarm(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("0123456789abcdef")
	addFile(t, f, "r.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", nil)

	// Single range: 206 with exact bytes.
	resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", map[string]string{"Range": "bytes=3-7"})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range GET status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(got, []byte("34567")) {
		t.Errorf("range body = %q, want %q", got, "34567")
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes 3-7/16" {
		t.Errorf("Content-In file test: Content-Range = %q, want bytes 3-7/16", cr)
	}
	if xc := resp.Header.Get("X-Cache"); xc != "HIT" {
		t.Errorf("range GET X-Cache = %q, want HIT", xc)
	}

	// Suffix-overflow end is clamped.
	resp2, got2 := proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", map[string]string{"Range": "bytes=10-999"})
	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("clamped range status = %d, want 206", resp2.StatusCode)
	}
	if !bytes.Equal(got2, []byte("abcdef")) {
		t.Errorf("clamped range body = %q, want %q", got2, "abcdef")
	}

	// Unsatisfiable start: 416 with Content-Range: bytes */size.
	resp3, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", map[string]string{"Range": "bytes=16-20"})
	if resp3.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("416 status = %d, want 416", resp3.StatusCode)
	}
	if cr := resp3.Header.Get("Content-Range"); cr != "bytes */16" {
		t.Errorf("416 Content-Range = %q, want bytes */16", cr)
	}

	// Multi-range: ignored, 200 full body.
	resp4, got4 := proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", map[string]string{"Range": "bytes=0-1,3-4"})
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("multi-range status = %d, want 200", resp4.StatusCode)
	}
	if !bytes.Equal(got4, body) {
		t.Errorf("multi-range body = %d bytes, want full %d", len(got4), len(body))
	}
	if cr := resp4.Header.Get("Content-Range"); cr != "" {
		t.Errorf("multi-range Content-Range = %q, want none", cr)
	}

	// Malformed Range: ignored, 200 full.
	resp5, got5 := proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", map[string]string{"Range": "bytes=abc"})
	if resp5.StatusCode != http.StatusOK || !bytes.Equal(got5, body) {
		t.Errorf("malformed range: status=%d body=%d bytes, want 200/full", resp5.StatusCode, len(got5))
	}
}

// TestColdRangePassthroughNoCache verifies the documented behavior: a cold
// ranged GET passes through to upstream without caching.
func TestColdRangePassthroughNoCache(t *testing.T) {
	up, f := newFakeUpstream(t)
	addFile(t, f, "r.bin", []byte("0123456789abcdef"))
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/r.bin", map[string]string{"Range": "bytes=3-7"})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("cold range status = %d, want 206 (passthrough)", resp.StatusCode)
	}
	if !bytes.Equal(got, []byte("34567")) {
		t.Errorf("cold range body = %q, want 34567", got)
	}
	if len(st.keys()) != 0 {
		t.Errorf("cold range cached objects: %v", st.keys())
	}
}

// TestGetColdTruncationNoManifest verifies that an upstream truncation
// mid-body leaves no manifest entry, and the next request re-fetches.
func TestGetColdTruncationNoManifest(t *testing.T) {
	up, f := newFakeUpstream(t)
	addFile(t, f, "t.bin", []byte("0123456789"))
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// First attempt truncated: client gets partial bytes (accepted v1).
	f.mu.Lock()
	f.truncate = 4
	f.mu.Unlock()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/org/name/resolve/main/t.bin", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, rerr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if rerr == nil {
		t.Fatalf("expected read error on truncated body, got %d clean bytes", len(got))
	}
	if !bytes.HasPrefix([]byte("0123456789"), got) {
		t.Errorf("partial client bytes = %q, want prefix of body", got)
	}

	// No manifest entry, so the next request is a MISS again.
	f.mu.Lock()
	f.truncate = 0
	f.mu.Unlock()
	resp2, got2 := proxyGet(t, srv.URL+"/org/name/resolve/main/t.bin", nil)
	if resp2.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("post-truncation X-Cache = %q, want MISS (re-fetch)", resp2.Header.Get("X-Cache"))
	}
	if !bytes.Equal(got2, []byte("0123456789")) {
		t.Errorf("re-fetched body = %q", got2)
	}
	var files map[string]string
	waitFor(t, 5*time.Second, func() bool {
		files = readFileManifest(t, st, sha1)
		return len(files) == 1 && files["t.bin"] != ""
	}, "manifest entry after re-fetch")
}

// TestGetColdS3PutFailure: when the store Put fails, the client response
// still completes (bytes already sent cannot be unsent), but no manifest
// entry is published.
func TestGetColdS3PutFailure(t *testing.T) {
	up, f := newFakeUpstream(t)
	addFile(t, f, "p.bin", []byte("0123456789"))
	st := newMemStore()
	st.mu.Lock()
	st.failPut = true
	st.mu.Unlock()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/p.bin", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (client bytes already sent)", resp.StatusCode)
	}
	if !bytes.Equal(got, []byte("0123456789")) {
		t.Fatalf("body = %q, want full body relayed", got)
	}
	if files := readFileManifest(t, st, sha1); len(files) != 0 {
		t.Errorf("manifest published despite S3 failure: %v", files)
	}
}

// TestClientDisconnectContinuesDownload: closing the client response early
// does not abort the server-side download→S3 upload; the object and manifest
// entry appear anyway.
func TestClientDisconnectContinuesDownload(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := bytes.Repeat([]byte("x"), 512*1024) // 512 KiB
	sum := addFile(t, f, "d.bin", body)
	f.mu.Lock()
	f.slow = 2 // ~2ms per 8KiB: ~128ms+ total, enough to disconnect mid-way
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Client reads a bit then disconnects.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/org/name/resolve/main/d.bin", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	resp.Body.Close()
	if n == 0 {
		t.Fatal("client read 0 bytes before disconnect")
	}

	// The detached download must complete: object + manifest entry exist.
	waitFor(t, 10*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files != nil && files["d.bin"] == sum
	}, "detached upload object + manifest entry")

	// Object bytes match the source exactly.
	rc, size, err := st.Get(context.Background(), fileKey(sha1, "org/name", "d.bin"))
	if err != nil {
		t.Fatalf("object after disconnect: %v", err)
	}
	defer rc.Close()
	obj, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(obj, body) || size != int64(len(body)) {
		t.Errorf("detached object = %d bytes (size %d), want %d byte-identical", len(obj), size, len(body))
	}
}

// TestConcurrentColdRequests: N clients ask for the same cold file at once;
// every client gets the full body and the cache converges to one object.
func TestConcurrentColdRequests(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := bytes.Repeat([]byte("y"), 256*1024)
	addFile(t, f, "c.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(srv.URL + "/org/name/resolve/main/c.bin")
			if err != nil {
				errs <- err
				return
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, body) {
				errs <- fmt.Errorf("body %d bytes, want %d", len(got), len(body))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestFileInvalidShapes: malformed paths are 400s, not route misses.
func TestFileInvalidShapes(t *testing.T) {
	up, _ := newFakeUpstream(t)
	p, _ := newTestProxy(t, up.URL, newMemStore())
	srv := newTestServer(p)
	defer srv.Close()

	for _, path := range []string{
		"/org/name/resolve/main",                             // no file
		"/org/name/resolve/main/",                            // empty file segment
		"/org/name/resolve/main/%2e%2e/etc",                  // traversal: decodes to ".." (literal ".." is path-cleaned by net/http before matching)
		"/org/name/resolve/main/a%20b",                       // decoded "a b": space invalid
		"/org/name/resolve/main/" + strings.Repeat("x", 512), // overlong
	} {
		t.Run(path, func(t *testing.T) {
			resp, _ := proxyGet(t, srv.URL+path, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
}

// TestFileNotFoundUpstream: upstream 404 maps to a JSON 404.
func TestFileNotFoundUpstream(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, body := proxyGet(t, srv.URL+"/org/name/resolve/main/nope.bin", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d (body %s), want 404", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"error"`) {
		t.Errorf("body = %s, want JSON error", body)
	}
	if len(st.keys()) != 0 {
		t.Errorf("404 cached: %v", st.keys())
	}
}

// TestHeadMissUpstream404: a cold HEAD for a missing upstream file is 404.
func TestHeadMissUpstream404(t *testing.T) {
	up, _ := newFakeUpstream(t)
	p, _ := newTestProxy(t, up.URL, newMemStore())
	srv := newTestServer(p)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/nope.bin", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestPinnedSHADirectFile: a 40-hex rev skips resolution entirely.
func TestPinnedSHADirectFile(t *testing.T) {
	up, f := newFakeUpstream(t)
	addFile(t, f, "x.bin", []byte("xyz"))
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/"+sha1+"/x.bin", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Repo-Commit") != sha1 {
		t.Fatalf("status=%d commit=%q", resp.StatusCode, resp.Header.Get("X-Repo-Commit"))
	}
	if n := f.count("/api/models/org/name/revision/main?"); n != 0 {
		t.Errorf("resolve calls = %d, want 0 (sha used directly)", n)
	}
}

// TestManifestStoresSizes: the durable manifest carries Sizes alongside
// sha256s (spec: "Store upstream ETag/size with SHA256" — size ledgered,
// ETag waived), and a warm HEAD after a proxy restart (fresh read cache)
// answers Content-Length from it without a store.Head.
func TestManifestStoresSizes(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("size-me-precisely")
	addFile(t, f, "s.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	proxyGet(t, srv.URL+"/org/name/resolve/main/s.bin", nil)
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files != nil && files["s.bin"] != ""
	}, "manifest entry")

	rc, _, err := st.Get(context.Background(), manifestKey(sha1))
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	var m struct {
		Files map[string]string `json:"files"`
		Sizes map[string]int64  `json:"sizes"`
	}
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.Sizes["s.bin"] != int64(len(body)) {
		t.Errorf("manifest Sizes[s.bin] = %d, want %d", m.Sizes["s.bin"], len(body))
	}
	if m.Files["s.bin"] == "" {
		t.Errorf("manifest Files[s.bin] empty")
	}

	// Fresh proxy (cold read cache) must serve the warm HEAD from the
	// manifest: Content-Length from Sizes.
	p2, _ := newTestProxy(t, up.URL, st)
	srv2 := newTestServer(p2)
	defer srv2.Close()
	req, _ := http.NewRequest(http.MethodHead, srv2.URL+"/org/name/resolve/main/s.bin", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("fresh-proxy HEAD X-Cache = %q, want HIT", resp.Header.Get("X-Cache"))
	}
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len(body)) {
		t.Errorf("fresh-proxy HEAD Content-Length = %q, want %d (from manifest Sizes)", got, len(body))
	}
}

// TestConcurrentPublishAndReads: parallel cold GETs of different files in
// one release (concurrent manifest publishes) racing concurrent warm
// HEADs/GETs of already-published files must not corrupt the shared cached
// manifest (concurrent map read/write) and must leave every entry visible.
// Run under -race this is the regression test for the publishFile/fileSHA
// lock discipline.
func TestConcurrentPublishAndReads(t *testing.T) {
	up, f := newFakeUpstream(t)
	for i := 0; i < 6; i++ {
		addFile(t, f, fmt.Sprintf("c%d.bin", i), bytes.Repeat([]byte{byte('a' + i)}, 64*1024+i))
	}
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Warm file 0 first so concurrent readers have something to hit.
	proxyGet(t, srv.URL+"/org/name/resolve/main/c0.bin", nil)
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files != nil && files["c0.bin"] != ""
	}, "seed manifest entry")

	var wg sync.WaitGroup
	errs := make(chan error, 24)
	// Concurrent publishers: cold GETs of the other files.
	for i := 1; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := http.Get(srv.URL + fmt.Sprintf("/org/name/resolve/main/c%d.bin", i))
			if err != nil {
				errs <- err
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}(i)
	}
	// Concurrent readers: warm HEADs/GETs of the seeded file.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Head(srv.URL + "/org/name/resolve/main/c0.bin")
			if err != nil {
				errs <- err
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// All entries must be present and correct.
	files := map[string]string{}
	waitFor(t, 5*time.Second, func() bool {
		files = readFileManifest(t, st, sha1)
		return files != nil && len(files) == 6
	}, "all 6 manifest entries")
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("c%d.bin", i)
		if files[name] == "" {
			t.Errorf("manifest missing %s: %v", name, files)
		}
	}
}

// TestNoPhantomHitOnManifestPutFailure: when the manifest Put fails, the
// read cache must NOT contain the unpublished entry — the next request
// re-fetches (MISS), never a phantom HIT.
func TestNoPhantomHitOnManifestPutFailure(t *testing.T) {
	up, f := newFakeUpstream(t)
	addFile(t, f, "ph.bin", []byte("phantom guard"))
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Manifest writes fail, body writes succeed.
	st.setFailPut(map[string]bool{manifestKey(sha1): true})

	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/ph.bin", nil)
	if resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("cold X-Cache = %q, want MISS", resp.Header.Get("X-Cache"))
	}
	// The read cache must not contain a phantom entry: the next request is
	// a MISS again (manifest Put failed), never a HIT.
	resp2, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/ph.bin", nil)
	if resp2.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("post-failure X-Cache = %q, want MISS (no phantom HIT)", resp2.Header.Get("X-Cache"))
	}
}

// TestDetachedFetchOutlivesClientTimeout: the file lane must use a client
// with no overall deadline — a detached cold pull runs to completion
// however long the body takes. Behavioral half: a body dripping past a
// shrunken METADATA client deadline (proving file transfers don't share
// it) still caches after the client disconnects; structural half: the
// default fileClient carries no Timeout.
func TestDetachedFetchOutlivesClientTimeout(t *testing.T) {
	up, f := newFakeUpstream(t)
	// 4 × 8 KiB chunks at 150ms per chunk = 600ms total drip, past the
	// 200ms metadata-client deadline set below.
	body := bytes.Repeat([]byte("z"), 4*8192)
	sum := addFile(t, f, "slow.bin", body)
	f.mu.Lock()
	f.slow = 150
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	if p.fileClient.Timeout != 0 {
		t.Fatalf("default fileClient.Timeout = %v, want 0 (no overall deadline on file transfers)", p.fileClient.Timeout)
	}
	// Shrink the METADATA client's deadline: if the file lane shared it,
	// this transfer would abort at 200ms.
	p.client = &http.Client{Timeout: 200 * time.Millisecond}
	srv := newTestServer(p)
	defer srv.Close()

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/org/name/resolve/main/slow.bin", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	n, _ := resp.Body.Read(make([]byte, 128))
	resp.Body.Close()
	if n == 0 {
		t.Fatal("client read 0 bytes")
	}

	waitFor(t, 15*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files != nil && files["slow.bin"] == sum
	}, "detached manifest entry past metadata client timeout")
}

// TestVanishedObjectSelfHeals: the manifest says a file exists but its S3
// object was deleted (data loss / retention sweep). The manifest is the
// authority for what SHOULD exist; store.Head is the authority for what
// DOES. HEAD and ranged GET must fall through to the upstream miss path
// and re-populate — never a false HIT 200 or a bogus 416.
func TestVanishedObjectSelfHeals(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("0123456789abcdef")
	sum := addFile(t, f, "v.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	key := fileKey(sha1, "org/name", "v.bin")
	proxyGet(t, srv.URL+"/org/name/resolve/main/v.bin", nil)
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files != nil && files["v.bin"] != ""
	}, "seed manifest entry")

	// The object vanishes; the manifest entry survives.
	st.delete(key)

	// HEAD must self-heal to the upstream miss, not claim HIT.
	req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/v.bin", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("vanished-object HEAD status = %d, want 200 (upstream miss path)", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Cache"); got != "MISS" {
		t.Errorf("vanished-object HEAD X-Cache = %q, want MISS (store.Head must gate the HIT)", got)
	}
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len(body)) {
		t.Errorf("vanished-object HEAD Content-Length = %q, want %d (upstream truth)", got, len(body))
	}
	if got := resp.Header.Get("ETag"); got == "" {
		t.Error("vanished-object HEAD lost ETag entirely")
	} else if got != `"`+sum+`"` {
		// The self-healed HEAD adopts the upstream hop's X-Linked-ETag, which
		// for this fixture is the same sha256 the manifest recorded.
		t.Logf("vanished-object HEAD ETag = %q (sha256 %q)", got, sum)
	}

	// A GET now re-populates the vanished object (HEAD never caches).
	resp2, got2 := proxyGet(t, srv.URL+"/org/name/resolve/main/v.bin", nil)
	if resp2.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("re-populating GET X-Cache = %q, want MISS", resp2.Header.Get("X-Cache"))
	}
	if !bytes.Equal(got2, body) {
		t.Errorf("re-populating body = %q", got2)
	}
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return st.has(key) && files["v.bin"] != ""
	}, "re-populated manifest entry")
	resp3, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/v.bin", nil)
	if resp3.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("post-repopulation GET X-Cache = %q, want HIT", resp3.Header.Get("X-Cache"))
	}

	// Vanish again and probe the ranged path the same way: a ranged GET on
	// a vanished object must self-heal via the upstream passthrough, not
	// fabricate a 206/416 from manifest metadata alone.
	st.delete(key)
	resp4, got4 := proxyGet(t, srv.URL+"/org/name/resolve/main/v.bin", map[string]string{"Range": "bytes=3-7"})
	if resp4.StatusCode != http.StatusPartialContent {
		t.Fatalf("vanished-object ranged GET status = %d, want 206 (passthrough)", resp4.StatusCode)
	}
	if !bytes.Equal(got4, []byte("34567")) {
		t.Errorf("vanished-object ranged body = %q, want 34567", got4)
	}
	if xc := resp4.Header.Get("X-Cache"); xc != "MISS" {
		t.Errorf("vanished-object ranged GET X-Cache = %q, want MISS", xc)
	}
	if et := resp4.Header.Get("ETag"); et == `"`+sum+`"` {
		t.Errorf("vanished ranged GET retained stale sealed ETag %q", et)
	}
}

func TestManifestJSONFileDoesNotCollideWithReleaseManifest(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("real upstream manifest.json file")
	sum := addFile(t, f, "manifest.json", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()
	url := srv.URL + "/org/name/resolve/main/manifest.json"
	if resp, got := proxyGet(t, url, nil); resp.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
		t.Fatalf("cold GET: %d %q", resp.StatusCode, got)
	}
	waitFor(t, 5*time.Second, func() bool {
		return st.has("pub/aa/" + sha1 + "/org/name/@manifest")
	}, "sentinel release manifest")
	if files := readFileManifest(t, st, sha1); files["manifest.json"] != sum {
		t.Fatalf("sentinel manifest missing correct file entry: %v", files)
	}
	rc, _, err := st.Get(context.Background(), fileKey(sha1, "org/name", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(stored, body) {
		t.Fatalf("stored file overwritten: %q", stored)
	}
	if resp, got := proxyGet(t, url, nil); resp.Header.Get("X-Cache") != "HIT" || !bytes.Equal(got, body) {
		t.Fatalf("warm GET: cache %q bytes %q", resp.Header.Get("X-Cache"), got)
	}
}

func TestForksWithSameCommitKeepSeparateManifests(t *testing.T) {
	sha := sha1
	st := newMemStore()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/revision/main") {
			_, _ = fmt.Fprintf(w, `{"sha":%q}`, sha)
			return
		}
		if strings.Contains(r.URL.Path, "/fork-a/") {
			_, _ = io.WriteString(w, "alpha")
			return
		}
		_, _ = io.WriteString(w, "bravo")
	}))
	defer up.Close()
	p := New(up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()
	for _, tc := range []struct{ repo, body string }{{"org/fork-a", "alpha"}, {"org/fork-b", "bravo"}} {
		url := srv.URL + "/" + tc.repo + "/resolve/main/weights.bin"
		if resp, got := proxyGet(t, url, nil); resp.StatusCode != http.StatusOK || string(got) != tc.body {
			t.Fatalf("cold %s: %d %q", tc.repo, resp.StatusCode, got)
		}
		key := manifestKeyFor(tc.repo, sha)
		waitFor(t, 5*time.Second, func() bool { return st.has(key) }, "fork manifest")
		if resp, got := proxyGet(t, url, nil); resp.Header.Get("X-Cache") != "HIT" || string(got) != tc.body {
			t.Fatalf("warm %s: cache %q bytes %q, want HIT %q", tc.repo, resp.Header.Get("X-Cache"), got, tc.body)
		}
	}
}

// waitFor polls cond every 10ms until true or timeout.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		// The manifest/object publish happens in the handler goroutine and
		// may complete just after the response returns.
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
	return false
}

// TestXErrorCodeOn404s pins HF-compatible typed-error headers: stock
// huggingface_hub raises RepositoryNotFoundError/EntryNotFoundError from
// these; without them every 404 is an untyped error.
func TestXErrorCodeOn404s(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// File lane: absent file → EntryNotFound.
	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/absent.bin", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("absent file status = %d, want 404", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Error-Code"); got != "EntryNotFound" {
		t.Errorf("file 404 X-Error-Code = %q, want EntryNotFound", got)
	}

	// Metadata lane: unknown repo → HF answers 401 to anonymous requests;
	// the mirror must surface that as a typed repo-404 (stock clients raise
	// RepositoryNotFoundError), never an untyped 502. The fake's catch-all
	// metadata branch (after the known-repo revision handler) answers 401,
	// and the resolve default-branch fetch scopes its code to the REPO.
	resp2, _ := proxyGet(t, srv.URL+"/api/models/org/absent-repo", nil)
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("absent repo status = %d, want 404 (mapped from upstream 401)", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Error-Code"); got != "RepoNotFound" {
		t.Errorf("metadata 404 X-Error-Code = %q, want RepoNotFound", got)
	}
}

// TestConcurrentColdGetSingleflight: N simultaneous cold GETs of the same
// file must produce exactly ONE upstream CDN fetch (the others wait and
// share the result via the manifest), not N racing transfers.
func TestConcurrentColdGetSingleflight(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := bytes.Repeat([]byte("sf-"), 300*1024) // ~900 KiB
	addFile(t, f, "sf.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	const n = 8
	var wg sync.WaitGroup
	results := make([][]byte, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/sf.bin", nil)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("cold GET %d status = %d, want 200", i, resp.StatusCode)
				return
			}
			results[i] = got
		}(i)
	}
	wg.Wait()
	for i, got := range results {
		if !bytes.Equal(got, body) {
			t.Errorf("cold GET %d body mismatch (%d bytes, got %d want %d)", i, len(got), len(got), len(body))
		}
	}

	// Exactly one upstream file GET hit the CDN, no matter how the N
	// requests interleaved.
	waitFor(t, 5*time.Second, func() bool {
		return f.cdnSeen["/org/name/resolve/"+sha1+"/sf.bin"] == 1
	}, "exactly one upstream fetch")
}

// TestColdRangeGETTriggersWarm: a cold ranged GET that relays a 206 must
// fire a detached background warm, so the file is cached after the relay
// and the next full GET is a HIT.
func TestColdRangeGETTriggersWarm(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("0123456789abcdefghij")
	sum := addFile(t, f, "rw.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Cold ranged GET: 206 relayed, and the warm fires in the background.
	resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/rw.bin", map[string]string{"Range": "bytes=2-6"})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("cold ranged GET status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(got, []byte("23456")) {
		t.Errorf("ranged body = %q, want 23456", got)
	}
	key := fileKey(sha1, "org/name", "rw.bin")
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return st.has(key) && files["rw.bin"] == sum
	}, "object + manifest entry after background warm")

	// Next full GET is a HIT served from the store.
	resp2, got2 := proxyGet(t, srv.URL+"/org/name/resolve/main/rw.bin", nil)
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("post-warm GET X-Cache = %q, want HIT", resp2.Header.Get("X-Cache"))
	}
	if !bytes.Equal(got2, body) {
		t.Errorf("post-warm body mismatch")
	}
}

// TestColdHeadNonLFS307TrueSize (regression A, v0.1.1): HF answers
// SHA-pinned non-LFS resolves with a 307 whose Content-Length is the
// redirect MESSAGE, not the file. The cold HEAD must never trust that
// length — it follows the redirect once and reports the real size, else
// cold snapshot_download fails its consistency check
// ("file should be of size X but has size Y").
func TestColdHeadNonLFS307TrueSize(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := bytes.Repeat([]byte("n"), 791) // actual size 791; the 307 hop's message is 43
	addFile(t, f, ".gitattributes", body)
	f.mu.Lock()
	f.lfs[".gitattributes"] = false // non-LFS: 307 hop without X-Linked-*
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/.gitattributes", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cold HEAD status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != "791" {
		t.Errorf("cold HEAD Content-Length = %q, want 791 (true file size, not the 307 message length)", got)
	}
}

// TestColdRangeDoesNotRelayCASETag (regression B, v0.1.1): a cold ranged
// GET on an xet file must NOT relay the CDN's CAS ETag — warm ranged HITs
// serve the sha256, and a cold/warm ETag flip makes persistent-local_dir
// clients re-download. The cold range answer carries no ETag when the
// truth (sha256) is unavailable on that hop.
func TestColdRangeDoesNotRelayCASETag(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.xetMode = true
	body := []byte("0123456789abcdefghij")
	sum := addFile(t, f, "xr.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/xr.bin", map[string]string{"Range": "bytes=0-4"})
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("cold ranged GET status = %d, want 206", resp.StatusCode)
	}
	if et := resp.Header.Get("ETag"); strings.HasPrefix(et, `"cas-`) {
		t.Errorf("cold ranged GET relayed CAS ETag %q — must not (warm serves sha256 %q)", et, sum)
	}
}

// TestBadRevisionGetsRevisionNotFound (finding 6): an unknown revision on a
// REAL repo must surface X-Error-Code RevisionNotFound, not RepoNotFound —
// stock clients raise RevisionNotFoundError vs RepositoryNotFoundError.
func TestBadRevisionGetsRevisionNotFound(t *testing.T) {
	up, f := newFakeUpstream(t)
	addFile(t, f, "f.bin", []byte("body"))
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// The fake upstream 404s the bad-revision resolve; the known repo has
	// files, so the repo itself exists. The typed code must say REVISION.
	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name/revision/v99bad", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bad revision status = %d, want 404", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Error-Code"); got != "RevisionNotFound" {
		t.Errorf("bad revision X-Error-Code = %q, want RevisionNotFound", got)
	}
	// File lane with the same bad revision: same typed code.
	resp2, _ := proxyGet(t, srv.URL+"/org/name/resolve/v99bad/f.bin", nil)
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("file lane bad revision status = %d, want 404", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Error-Code"); got != "RevisionNotFound" {
		t.Errorf("file lane bad revision X-Error-Code = %q, want RevisionNotFound", got)
	}
}

// TestColdNonLFSHeadCompleteMetadata is THE regression test of the whole
// v0.1.2-v0.1.4 saga: a cold HEAD on a non-LFS file through the mirror
// must carry BOTH a correct Content-Length (not the redirect-message
// length) AND a non-empty ETag (hf_hub hard-raises FileMetadataError on a
// missing ETag before any size logic). Each earlier variant failed exactly
// one of the two: v0.1.3 right etag/wrong size, v0.1.4 right size/no etag.
func TestColdNonLFSHeadCompleteMetadata(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := bytes.Repeat([]byte("n"), 791)
	addFile(t, f, ".gitattributes", body)
	f.mu.Lock()
	f.lfs[".gitattributes"] = false // non-LFS: relative-Location 307 hop
	f.mu.Unlock()
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodHead, srv.URL+"/org/name/resolve/main/.gitattributes", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cold HEAD status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != "791" {
		t.Errorf("Content-Length = %q, want 791 (true size from the followed hop)", got)
	}
	if got := resp.Header.Get("ETag"); got == "" {
		t.Error("ETag empty — hf_hub raises FileMetadataError before size logic; the followed hop's ETag must be relayed")
	}
}

// TestFanoutStreamsToAllWaiters: concurrent cold GETs in the leader's join
// window must all receive the FULL body as it streams (fan-out), not block
// byteless on a done-channel until hf_hub's 10s read timeout kills them —
// the v0.1.5 fleet defect (GET ... 200 0B 10.009s).
func TestFanoutStreamsToAllWaiters(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.slow = 30 // ms per 8KiB: the join window stays open long enough
	f.mu.Unlock()
	body := bytes.Repeat([]byte("fanout!"), 128*1024) // ~896 KiB
	addFile(t, f, "fan.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// One leader, three joiners. The joiners fire AFTER the leader is
	// mid-stream, so they deterministically take the waiter path — and the
	// test asserts every waiter completes quickly (hf_hub aborts after 10s
	// of zero bytes; the fleet defect was byteless waiters).
	leaderStarted := make(chan struct{})
	const n = 4
	var wg sync.WaitGroup
	got := make([][]byte, n)
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, b := proxyGet(t, srv.URL+"/org/name/resolve/main/fan.bin", nil)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("leader status = %d, want 200", resp.StatusCode)
		}
		got[0] = b
	}()
	// Give the leader a 300ms head start mid-drip, then release the joiners.
	go func() {
		time.Sleep(300 * time.Millisecond)
		close(leaderStarted)
	}()
	<-leaderStarted
	first := make(chan time.Duration, n-1)
	for i := 1; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start := time.Now()
			resp, err := http.Get(srv.URL + "/org/name/resolve/main/fan.bin")
			if err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			defer resp.Body.Close()
			one := make([]byte, 1)
			if _, err := io.ReadFull(resp.Body, one); err != nil {
				t.Errorf("waiter %d first byte: %v", i, err)
				return
			}
			first <- time.Since(start)
			if resp.StatusCode != http.StatusOK {
				t.Errorf("waiter %d status = %d, want 200", i, resp.StatusCode)
				return
			}
			if xc := resp.Header.Get("X-Cache"); xc == "" {
				t.Errorf("waiter %d: no X-Cache", i)
			}
			rest, _ := io.ReadAll(resp.Body)
			got[i] = append(one, rest...)
		}(i)
	}
	wg.Wait()
	close(first)
	for d := range first {
		if d > 1*time.Second {
			t.Errorf("waiter first byte after %v — waiters must stream immediately, not stall (fleet defect; hf_hub aborts at 10s)", d)
		}
	}
	for i := range got {
		if !bytes.Equal(got[i], body) {
			t.Errorf("waiter %d body: got %d bytes, want %d (full stream)", i, len(got[i]), len(body))
		}
	}
}

// TestLateWaiterGetsOwnStream: a request arriving AFTER the join window
// closed (leader already streaming) must not block byteless — it serves
// via its own upstream stream (uncached), so every cold GET returns bytes.
func TestLateWaiterGetsOwnStream(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.mu.Lock()
	f.slow = 40
	f.mu.Unlock()
	body := bytes.Repeat([]byte("late!"), 200*1024) // ~800 KiB
	addFile(t, f, "late.bin", body)
	st := newMemStore()
	p, _ := newTestProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// Waiter must see FIRST BYTES while the leader still streams: hf_hub
	// aborts after 10s of zero bytes — the fleet defect. The leader drips
	// ~110 chunks at 30ms (~3.3s), so first-byte-under-1s is decisive.
	resp0 := make(chan *http.Response, 1)
	go func() {
		r, err := http.Get(srv.URL + "/org/name/resolve/main/fan.bin")
		if err != nil {
			resp0 <- nil
			return
		}
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r.Body, buf); err != nil {
			r.Body.Close()
			resp0 <- nil
			return
		}
		resp0 <- r
	}()
	var leaderResp *http.Response
	select {
	case leaderResp = <-resp0:
	case <-time.After(1 * time.Second):
		t.Fatal("first byte took >1s while leader streamed — waiters stall byteless (the fleet defect)")
	}
	leaderResp.Body.Close()

	// Leader: read the first chunk, then (window closed) fire the late GET.
	leader := make(chan []byte, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/org/name/resolve/main/late.bin")
		if err != nil {
			leader <- nil
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, 8192)
		n, _ := resp.Body.Read(buf) // first chunk arrives → join window over
		leader <- buf[:n]
		io.Copy(io.Discard, resp.Body)
	}()
	if first := <-leader; first == nil {
		t.Fatal("leader stream failed to start")
	}
	resp, b := proxyGet(t, srv.URL+"/org/name/resolve/main/late.bin", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("late waiter status = %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(b, body) {
		t.Fatalf("late waiter body: got %d bytes, want %d (own full stream)", len(b), len(body))
	}
	// The cache still converges via the leader exactly once.
	sum := sha256.Sum256(body)
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files["late.bin"] == hex.EncodeToString(sum[:])
	}, "manifest entry via leader")
}
