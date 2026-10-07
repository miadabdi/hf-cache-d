//go:build s3compose

// Integration test running the proxy against a fake upstream and the real
// SeaweedFS store from docker-compose (same probe/skip pattern as
// internal/store). Verifies the same end-to-end caching contract as the
// offline tests, but through actual S3 persistence.

package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/store"
)

// s3Endpoint returns the integration endpoint, S3_TEST_ENDPOINT or the
// compose default (8333; this machine remaps the host port to 18333).
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

func newComposeStore(t *testing.T) *store.Store {
	t.Helper()
	endpoint := s3Endpoint()
	hostport := endpoint
	for _, prefix := range []string{"http://", "https://"} {
		if len(hostport) >= len(prefix) && hostport[:len(prefix)] == prefix {
			hostport = hostport[len(prefix):]
		}
	}
	conn, err := net.DialTimeout("tcp", hostport, 2*time.Second)
	if err != nil {
		t.Skipf("S3 endpoint %s not reachable (start with: docker compose up -d && ./scripts/dev-s3.sh): %v", endpoint, err)
	}
	conn.Close()

	st, err := store.New(endpoint, envOr("S3_BUCKET", "test-bucket"), envOr("S3_ACCESS_KEY", "test"), envOr("S3_SECRET_KEY", "test12345678"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	// Reachable but wrong owner (foreign SeaweedFS squatting the port)
	// surfaces as a non-NotFound error: skip with instructions.
	if _, _, err := st.Get(context.Background(), "auth-probe"); err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Skipf("S3 endpoint %s reachable but not usable with fixture creds (expected when another SeaweedFS owns the port; fix with: docker compose down && SEAWEEDFS_S3_PORT=8333 docker compose up -d && ./scripts/dev-s3.sh): %v", endpoint, err)
	}
	return st
}

func TestProxyAgainstRealStore(t *testing.T) {
	up, f := newFakeUpstream(t)
	// Unique repo per run: the compose bucket is shared and never wiped, so
	// deterministic keys would make the second run start cache-warm.
	f.setRepo(fmt.Sprintf("org/name-%d", time.Now().UnixNano()))
	st := newComposeStore(t)
	p := New(up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	repo := "/api/models/" + f.repoOf()
	resp1, body1 := proxyGet(t, srv.URL+repo, nil)
	if resp1.StatusCode != 200 || resp1.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("cold: status=%d X-Cache=%q", resp1.StatusCode, resp1.Header.Get("X-Cache"))
	}
	resp2, body2 := proxyGet(t, srv.URL+repo, nil)
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Errorf("warm X-Cache = %q, want HIT", resp2.Header.Get("X-Cache"))
	}
	if string(body1) != string(body2) {
		t.Error("cached body differs from first response")
	}
	if n := f.count("/api/models/" + f.repoOf() + "/revision/" + sha1 + "?"); n != 1 {
		t.Errorf("pinned fetches = %d, want 1", n)
	}

	resp3, _ := proxyGet(t, srv.URL+repo+"/tree/main", nil)
	if resp3.StatusCode != 200 || resp3.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("tree cold: status=%d X-Cache=%q", resp3.StatusCode, resp3.Header.Get("X-Cache"))
	}
	resp4, _ := proxyGet(t, srv.URL+repo+"/tree/main", nil)
	if resp4.Header.Get("X-Cache") != "HIT" {
		t.Errorf("tree warm X-Cache = %q, want HIT", resp4.Header.Get("X-Cache"))
	}
}

// TestFileLaneAgainstRealStore exercises the resolve lane end to end through
// real S3: cold GET tee'd into multipart/put streaming, manifest published,
// warm GET byte-identical with zero upstream file traffic, warm Range from
// store.GetRange.
func TestFileLaneAgainstRealStore(t *testing.T) {
	up, f := newFakeUpstream(t)
	f.setRepo(fmt.Sprintf("org/name-%d", time.Now().UnixNano()))
	// Unique commit sha per run: packages under ./... run in parallel
	// against one shared bucket, and the manifest key carries only the sha
	// (a commit sha identifies one repo in reality), so fixtures must not
	// share one either.
	uniq := sha256.Sum256([]byte(f.repoOf()))
	sha := hex.EncodeToString(uniq[:20]) // 40-hex, the commit shape
	f.mu.Lock()
	f.main = sha
	f.mu.Unlock()
	repo := f.repoOf()
	st := newComposeStore(t)
	p := New(up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	body := bytes.Repeat([]byte("0123456789abcdef"), 512*1024) // 8 MiB
	f.mu.Lock()
	f.files["big.bin"] = body
	f.files["nested/dir/file.txt"] = []byte("nested")
	f.mu.Unlock()
	sum := sha256.Sum256(body)

	// Cold GET: MISS, byte-identical, streamed into the store.
	resp1, got1 := proxyGet(t, srv.URL+"/"+repo+"/resolve/main/big.bin", nil)
	if resp1.StatusCode != 200 || resp1.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("cold: status=%d X-Cache=%q", resp1.StatusCode, resp1.Header.Get("X-Cache"))
	}
	if !bytes.Equal(got1, body) {
		t.Fatalf("cold body: %d bytes, want %d byte-identical", len(got1), len(body))
	}

	// Manifest entry published (poll: publish trails the response).
	deadline := time.Now().Add(10 * time.Second)
	var files map[string]string
	for time.Now().Before(deadline) {
		rc, _, err := st.Get(context.Background(), fmt.Sprintf("pub/%s/%s/manifest.json", sha[:2], sha))
		if err == nil {
			var m struct {
				Files map[string]string `json:"files"`
			}
			if jerr := json.NewDecoder(rc).Decode(&m); jerr == nil {
				files = m.Files
			}
			rc.Close()
			if files["big.bin"] == hex.EncodeToString(sum[:]) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if files["big.bin"] != hex.EncodeToString(sum[:]) {
		t.Fatalf("manifest after cold GET = %v, want big.bin=%s", files, hex.EncodeToString(sum[:]))
	}

	// Warm GET: HIT, zero upstream file traffic, byte-identical.
	f.mu.Lock()
	f.cdnSeen = map[string]int{}
	f.mu.Unlock()
	resp2, got2 := proxyGet(t, srv.URL+"/"+repo+"/resolve/main/big.bin", nil)
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("warm X-Cache = %q, want HIT", resp2.Header.Get("X-Cache"))
	}
	if !bytes.Equal(got2, body) {
		t.Error("warm body differs from cold body")
	}
	if et := resp2.Header.Get("ETag"); et != `"`+hex.EncodeToString(sum[:])+`"` {
		t.Errorf("warm ETag = %q, want quoted sha256", et)
	}
	if n := len(f.cdnSeen); n != 0 {
		t.Errorf("upstream file traffic after warm = %d requests", n)
	}

	// Warm Range served from the store: exact bytes.
	resp3, got3 := proxyGet(t, srv.URL+"/"+repo+"/resolve/main/big.bin", map[string]string{"Range": "bytes=3-7"})
	if resp3.StatusCode != http.StatusPartialContent {
		t.Fatalf("warm range status = %d, want 206", resp3.StatusCode)
	}
	if !bytes.Equal(got3, body[3:8]) {
		t.Errorf("warm range bytes = %q", got3)
	}

	// Nested path cold pull: separate file entry.
	proxyGet(t, srv.URL+"/"+repo+"/resolve/main/nested/dir/file.txt", nil)

	// A second full pull pass hits everything from the store only.
	f.mu.Lock()
	f.cdnSeen = map[string]int{}
	f.mu.Unlock()
	proxyGet(t, srv.URL+"/"+repo+"/resolve/main/big.bin", nil)
	proxyGet(t, srv.URL+"/"+repo+"/resolve/main/nested/dir/file.txt", nil)
	if n := len(f.cdnSeen); n != 0 {
		t.Errorf("second pass upstream file traffic = %d requests, want 0", n)
	}
}
