// Package local tracks private models sealed by the push lane (Task 4) and
// serves the key layout shared by push and proxy:
//
//	priv/<org>/<name>/<version>/files/<file>     file bodies (immutable after seal)
//	priv/<org>/<name>/<version>/manifest.json    seal manifest; presence = sealed
//	priv/<org>/<name>/index.json                 repo version index {versions, main}
//
// The index cache answers "is this repo a sealed local model?" and "which
// seal does rev mean?" without touching S3 on the hot path; seal refreshes it.
package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/store"
)

// Version is one sealed version in a repo index.
type Version struct {
	Version  string    `json:"version"`
	Commit   string    `json:"commit"` // synthetic 40-hex
	SealedAt time.Time `json:"sealedAt"`
}

// Index is the repo-level pointer file at priv/<org>/<name>/index.json.
type Index struct {
	Versions []Version `json:"versions"` // ordered by seal time (oldest first)
	Main     string    `json:"main"`     // commit of the most recently sealed version
}

// IndexKey is the object key of a repo's index.
func IndexKey(repo string) string { return "priv/" + repo + "/index.json" }

// ManifestKey is the seal manifest object key for repo@version.
func ManifestKey(repo, version string) string {
	return "priv/" + repo + "/" + version + "/manifest.json"
}

// FileKey is the staged/immortal body key for repo@version/file.
func FileKey(repo, version, file string) string {
	return "priv/" + repo + "/" + version + "/files/" + file
}

// Indexes is the lazily-loaded, seal-refreshed cache of repo indexes. The
// proxy consults it before the public flow; the push lane refreshes it after
// a successful seal. An absent index means "not a local model" and is cached
// as a nil entry so cold unknown repos cost one S3 read, not one per request.
// ponytail: single-process memory cache; a second daemon against the same
// bucket would need to drop it (or add polling) to stay coherent.
type Indexes struct {
	store storeAPI

	mu sync.Mutex
	// repo -> index; nil value = known-absent. Absent from the map = not
	// looked up yet (first request pays one index read).
	m map[string]*Index
}

// storeAPI is the narrow store slice local consumes (same shape as the
// proxy's, duplicated because proxy's is unexported).
type storeAPI interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Head(ctx context.Context, key string) (bool, int64, error)
}

// NewIndexes builds the index cache.
func NewIndexes(st storeAPI) *Indexes {
	return &Indexes{store: st, m: map[string]*Index{}}
}

// Get returns the repo's index, or nil when the repo has no sealed versions.
// A transient store failure yields an error (NOT a nil index): callers must
// fail the request rather than fall back to the public flow, and the
// not-local verdict is never cached from a failed read — the next request
// retries. A read that raced a concurrent Refresh defers to the installed
// (newer) view instead of caching its own stale result.
func (ix *Indexes) Get(ctx context.Context, repo string) (*Index, error) {
	ix.mu.Lock()
	if idx, seen := ix.m[repo]; seen {
		ix.mu.Unlock()
		return idx, nil
	}
	ix.mu.Unlock()

	idx, err := readIndex(ctx, ix.store, repo)
	if err != nil {
		return nil, err
	}
	ix.mu.Lock()
	if cur, seen := ix.m[repo]; seen {
		// A seal refreshed the index while this read was in flight: the
		// installed view is newer than what we just read.
		ix.mu.Unlock()
		return cur, nil
	}
	ix.m[repo] = idx
	ix.mu.Unlock()
	return idx, nil
}

// Refresh installs idx as repo's cached index (used by the push lane after
// a successful seal so the proxy sees the new version without a store read).
func (ix *Indexes) Refresh(repo string, idx *Index) {
	ix.mu.Lock()
	ix.m[repo] = idx
	ix.mu.Unlock()
}

// readIndex loads one repo index. A missing index is (nil, nil) — genuinely
// not a local model. Any other failure (backend error, corrupt JSON) is an
// error: treating it as "not local" would let a sealed repo fall through to
// the public upstream, which the shadowing rule forbids.
func readIndex(ctx context.Context, st storeAPI, repo string) (*Index, error) {
	rc, _, err := st.Get(ctx, IndexKey(repo))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("local index read %s: %w", repo, err)
	}
	defer rc.Close()
	var idx Index
	if err := json.NewDecoder(rc).Decode(&idx); err != nil {
		log.Printf("local index for %s not valid: %v", repo, err)
		return nil, fmt.Errorf("local index %s is corrupt", repo)
	}
	if len(idx.Versions) == 0 {
		return nil, nil
	}
	return &idx, nil
}

// Lookup maps rev to a sealed version of repo.
//   - "" or "main": the most recently sealed version (by seal time, not
//     version ordering), or ok=false when none exists.
//   - a version string: that exact seal.
//   - a 40-hex string: the seal whose synthetic commit matches; never
//     anything else (the shadowing rule: no fallback to public).
//
// Any rev that matches nothing is ok=false; the caller turns that into a 404
// for repos with at least one seal.
func (idx *Index) Lookup(rev string) (Version, bool) {
	if len(idx.Versions) == 0 {
		return Version{}, false
	}
	if rev == "" || rev == "main" {
		// Versions are stored oldest-first; the last entry is latest-sealed.
		last := idx.Versions[len(idx.Versions)-1]
		return last, true
	}
	for _, v := range idx.Versions {
		if v.Version == rev || v.Commit == rev {
			return v, true
		}
	}
	return Version{}, false
}

// CommitOf derives the synthetic 40-hex commit: sha256 over the canonical
// JSON encoding of the manifest's content-bearing fields (identity, files,
// sizes), first 40 hex chars. encoding/json emits struct fields in
// declaration order and map keys sorted, so the encoding is canonical —
// identical content yields identical commits across restarts and re-seals.
// PulledAt is deliberately excluded (it changes per seal).
func CommitOf(identity string, files map[string]string, sizes map[string]int64) string {
	if sizes == nil {
		sizes = map[string]int64{} // canonical: {} not null
	}
	b, err := json.Marshal(struct {
		Identity string            `json:"identity"`
		Files    map[string]string `json:"files"`
		Sizes    map[string]int64  `json:"sizes"`
	}{identity, files, sizes})
	if err != nil {
		// These value types cannot fail to marshal; sha of nothing keeps the
		// function total.
		b = nil
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:20])
}
