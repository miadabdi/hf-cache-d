//go:build s3compose

// End-to-end test of the snapshot pull sequence against the real routing
// table (newMux) and the compose SeaweedFS store: metadata → 3 files →
// repeat; pass 2 must make zero upstream file requests and serve identical
// bytes, and the durable manifest must carry the right sha256s.
// Shared fixtures (fakeHub, pushLane, mustPut, mustSeal) live in
// helpers_test.go.

package test

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
	"sync/atomic"
	"testing"
	"time"
)

// TestSnapshotSequenceE2E: the full metadata→files→repeat flow.
func TestSnapshotSequenceE2E(t *testing.T) {
	st := newComposeStore(t)
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
	srv := pushLane(t, st, f.upstream, "")

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
		if pass == 2 {
			// Deterministic barrier: pass 1's detached publish (object Put +
			// manifest RMW) may still be in flight when the responses
			// returned. Every file must be manifest-published and its object
			// headed before pass 2 can assert HIT/zero-traffic — otherwise
			// correct code fails the test.
			barrier(t, st, f.sha, repo, files)
		}

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
	rc, _, err := st.Get(context.Background(), fmt.Sprintf("pub/%s/%s/%s/@manifest", f.sha[:2], f.sha, repo))
	if err != nil {
		t.Fatalf("manifest get: %v", err)
	}
	defer rc.Close()
	var m struct {
		Identity string            `json:"identity"`
		Files    map[string]string `json:"files"`
		Sizes    map[string]int64  `json:"sizes"`
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
		if m.Sizes[file] != int64(len(files[file])) {
			t.Errorf("manifest Sizes[%s] = %d, want %d", file, m.Sizes[file], len(files[file]))
		}
	}

	// Pass 2 made zero upstream FILE requests (metadata ref hit its TTL).
	if n := f.cdnCount(); n != len(paths) {
		t.Errorf("upstream CDN file requests total = %d, want %d (one per file, pass 1 only)", n, len(paths))
	}
}

// TestPrivatePushRoundtripE2E: PUT 2 files → seal → snapshot-shaped GET
// sequence twice through the HF routes; pass 2 makes zero upstream requests
// and serves byte-identical bytes from the sealed objects.
func TestPrivatePushRoundtripE2E(t *testing.T) {
	// Fake public upstream that must NEVER be consulted for the local repo.
	var upstreamHits int32
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(pub.Close)

	st := newComposeStore(t)
	repo := fmt.Sprintf("org/push-e2e-%d", time.Now().UnixNano())
	srv := pushLane(t, st, pub.URL, "sekrit")

	big := bytes.Repeat([]byte("push-big-"), 512*1024) // ~4.6 MiB
	small := []byte("push-small")
	sums := map[string]string{}
	sizes := map[string]int64{}
	for name, body := range map[string][]byte{"big.bin": big, "small.bin": small} {
		sums[name] = mustPut(t, srv.URL, repo, "v1", name, body)
		sizes[name] = int64(len(body))
	}
	commit := mustSeal(t, srv.URL, repo, "v1", sums, sizes)
	if len(commit) != 40 {
		t.Fatalf("commit = %q, want 40-hex", commit)
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
		info, _ := http.Get(srv.URL + "/api/models/" + repo)
		info.Body.Close()
		if info.StatusCode != 200 {
			t.Fatalf("pass %d: info status %d", pass, info.StatusCode)
		}
		var infoBody struct {
			SHA string `json:"sha"`
		}
		// info served byte-identical both passes
		_, raw := getAux(t, srv.URL+"/api/models/"+repo)
		_ = json.Unmarshal(raw, &infoBody)
		if infoBody.SHA != commit {
			t.Errorf("pass %d: info sha = %s, want %s", pass, infoBody.SHA, commit)
		}

		for name, want := range map[string][]byte{"big.bin": big, "small.bin": small} {
			resp, body := get(name)
			if resp.StatusCode != 200 {
				t.Fatalf("pass %d %s: status %d", pass, name, resp.StatusCode)
			}
			if !bytes.Equal(body, want) {
				t.Fatalf("pass %d %s: body %d bytes, want %d byte-identical", pass, name, len(body), len(want))
			}
			if et := resp.Header.Get("ETag"); et != `"`+sums[name]+`"` {
				t.Errorf("pass %d %s: ETag = %q, want stored sha256", pass, name, et)
			}
			if got := resp.Header.Get("X-Repo-Commit"); got != commit {
				t.Errorf("pass %d %s: X-Repo-Commit = %s, want %s", pass, name, got, commit)
			}
		}
	}

	// Zero public-upstream requests across the whole sequence: the sealed
	// model shadows the public repo, and both passes served from the store.
	if n := atomic.LoadInt32(&upstreamHits); n != 0 {
		t.Errorf("public upstream requests = %d, want 0 (sealed local shadowing)", n)
	}

	// Restart persistence against the real store: fresh lanes, same bucket.
	srv2 := pushLane(t, st, pub.URL, "sekrit")
	resp, body := httpGet(t, srv2.URL+"/"+repo+"/resolve/main/big.bin")
	if resp.StatusCode != 200 || !bytes.Equal(body, big) {
		t.Errorf("restart GET = %d (%d bytes), want 200 byte-identical", resp.StatusCode, len(body))
	}
	if n := atomic.LoadInt32(&upstreamHits); n != 0 {
		t.Errorf("restart public upstream requests = %d, want 0", n)
	}
}

func getAux(t *testing.T, rawURL string) (*http.Response, []byte) {
	t.Helper()
	return httpGet(t, rawURL)
}

func httpGet(t *testing.T, rawURL string) (*http.Response, []byte) {
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
