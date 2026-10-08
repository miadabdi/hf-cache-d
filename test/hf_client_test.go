//go:build client

// Real-client compatibility suite: the UNMODIFIED system huggingface_hub
// (tested pin: 0.36.2 on Python 3.14, run against the system install — no
// venv, no internet in the default test path) pulling snapshots through the
// service with endpoint=<service URL>.
//
// Run (needs compose SeaweedFS, hence the s3probe-skip contract):
//
//	SEAWEEDFS_S3_PORT=18333 go test -tags client ./test/ -run TestHFClient -v
//
// Python must be importable as `python3`. Compat is verified against exactly
// the pinned version; adding a second supported version requires internet to
// verify its API (documented limitation, see task-5 report).

package test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// runClient drives one python3 snapshot_download through the service.
// hfHome gets a fresh temp dir per call: the Python cache can never mask
// mirror behavior (the "clear python cache" requirement).
func runClient(t *testing.T, endpoint, repo, revision, localDir string, expectError bool) {
	t.Helper()
	hfHome := t.TempDir()
	args := []string{"hf_client.py", endpoint, repo, revision, localDir} // cwd = test/
	if expectError {
		args = append(args, "expect-error")
	}
	// No -I: isolated mode would drop the user site-packages the system
	// huggingface_hub lives in. The script dir is this repo's test/ (trusted).
	cmd := exec.Command("python3", args...)
	cmd.Env = append(os.Environ(),
		"HF_HOME="+hfHome,
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_HUB_DISABLE_PROGRESS_BARS=1",
		"HF_HUB_OFFLINE=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python driver for %s@%s failed: %v\n%s", repo, revision, err, out)
	}
	if expectError {
		if !strings.Contains(string(out), "EXPECTED_FAILURE") || strings.Contains(string(out), "UNEXPECTED_SUCCESS") {
			t.Fatalf("python driver for %s@%s: expected a raised error, got:\n%s", repo, revision, out)
		}
		if !strings.Contains(string(out), "404") {
			t.Errorf("python driver for %s@%s: error does not mention 404:\n%s", repo, revision, out)
		}
		if !strings.Contains(strings.ToLower(string(out)), "revision") {
			t.Errorf("python driver for %s@%s: error does not mention the revision:\n%s", repo, revision, out)
		}
		return
	}
	if !strings.Contains(string(out), "DONE") {
		t.Fatalf("python driver for %s@%s: no DONE line:\n%s", repo, revision, out)
	}
}

// assertDirBytes asserts the downloaded snapshot in dir is exactly want:
// every file byte-identical, and no file outside the expected set (the
// client's .cache/huggingface metadata is tolerated and ignored).
func assertDirBytes(t *testing.T, dir string, want map[string][]byte) {
	t.Helper()
	for name, body := range want {
		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("downloaded %s: %v", name, err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("downloaded %s: %d bytes, want %d byte-identical", name, len(got), len(body))
		}
	}
	seen := map[string]bool{}
	for name := range want {
		seen[name] = true
	}
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		rel = filepath.ToSlash(rel)
		if !seen[rel] && !strings.HasPrefix(rel, ".cache/") {
			t.Errorf("unexpected downloaded file %s", rel)
		}
		return nil
	})
}

// TestHFClientPublicSnapshot: cold snapshot_download of a public (fake
// upstream) model lands byte-identical files; a WARM run in a fresh HF_HOME
// and fresh local_dir (Python cache provably empty) still completes and
// makes zero upstream file requests — the mirror served everything.
func TestHFClientPublicSnapshot(t *testing.T) {
	st := newComposeStore(t)

	repo := fmt.Sprintf("org/cli-pub-%d", time.Now().UnixNano())
	big := bytes.Repeat([]byte("cli-big-"), 1<<19) // 4 MiB, streamed
	files := map[string][]byte{
		"big.bin":              big,
		"small.bin":            []byte("tiny client file"),
		"nested/deep/file.txt": []byte("nested client body"),
	}
	f := newFakeHub(t, repo, files)
	srv := pushLane(t, st, f.upstream, "")

	// Cold pull.
	cold := t.TempDir()
	runClient(t, srv.URL, repo, "main", cold, false)
	assertDirBytes(t, cold, files)
	cdnAfterCold := f.cdnCount()

	// Warm pull: barrier FIRST (the cold pull's detached publish must be
	// durable or the warm pull would legitimately re-fetch), then a FRESH
	// python cache + fresh dir, then assert zero new upstream file requests.
	barrier(t, st, f.sha, repo, files)
	warm := t.TempDir()
	runClient(t, srv.URL, repo, "main", warm, false)
	assertDirBytes(t, warm, files)
	if n := f.cdnCount(); n != cdnAfterCold {
		t.Errorf("upstream CDN file requests after warm pull = %d, want %d (warm served by mirror)", n, cdnAfterCold)
	}
}

// TestHFClientSealedModel: PUT+seal a local model, then stock-client pulls:
// main (latest seal), explicit v2, synthetic commit; unknown revision raises
// a 404-flavored error; and the public upstream is never contacted for the
// local repo across the whole sequence (shadowing).
func TestHFClientSealedModel(t *testing.T) {
	st := newComposeStore(t)

	var upstreamHits int32
	pub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(pub.Close)

	repo := fmt.Sprintf("org/cli-priv-%d", time.Now().UnixNano())
	srv := pushLane(t, st, pub.URL, "sekrit")

	big := bytes.Repeat([]byte("cli-priv-"), 512*1024) // ~4 MiB, ≥2 MiB per brief
	v1Files := map[string][]byte{"big.bin": big, "notes.txt": []byte("v1 notes")}
	v2Files := map[string][]byte{"big.bin": big, "notes.txt": []byte("v2 notes")}
	v2Commit := sealVersion(t, srv.URL, repo, "v2", v2Files)

	// main = latest seal = v2.
	main := t.TempDir()
	runClient(t, srv.URL, repo, "main", main, false)
	assertDirBytes(t, main, v2Files)

	// Explicit v2 selects exactly v2 (and a v1 seal would select v1).
	sealVersion(t, srv.URL, repo, "v1", v1Files)
	v1 := t.TempDir()
	runClient(t, srv.URL, repo, "v1", v1, false)
	assertDirBytes(t, v1, v1Files)

	// The synthetic commit pins the v2 bytes even after v1 was sealed later.
	byCommit := t.TempDir()
	runClient(t, srv.URL, repo, v2Commit, byCommit, false)
	assertDirBytes(t, byCommit, v2Files)

	// Unknown revision: stock client raises, error mentions 404 + revision.
	runClient(t, srv.URL, repo, "v99", t.TempDir(), true)

	// Shadowing: zero public-upstream requests for the local repo, total.
	if n := atomic.LoadInt32(&upstreamHits); n != 0 {
		t.Errorf("public upstream requests for local model = %d, want 0", n)
	}
}

// sealVersion PUTs files through the push lane and seals; returns the
// synthetic commit.
func sealVersion(t *testing.T, base, repo, version string, files map[string][]byte) string {
	t.Helper()
	sums := map[string]string{}
	sizes := map[string]int64{}
	for name, body := range files {
		sums[name] = mustPut(t, base, repo, version, name, body)
		sizes[name] = int64(len(body))
	}
	return mustSeal(t, base, repo, version, sums, sizes)
}
