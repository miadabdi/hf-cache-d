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
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"
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
func (ix *Indexes) Get(ctx context.Context, repo string) *Index {
	ix.mu.Lock()
	if idx, seen := ix.m[repo]; seen {
		ix.mu.Unlock()
		return idx
	}
	ix.mu.Unlock()

	idx := readIndex(ctx, ix.store, repo)
	ix.mu.Lock()
	if _, seen := ix.m[repo]; !seen {
		ix.m[repo] = idx
	}
	ix.mu.Unlock()
	return idx
}

// Refresh installs idx as repo's cached index (used by the push lane after
// a successful seal so the proxy sees the new version without a store read).
func (ix *Indexes) Refresh(repo string, idx *Index) {
	ix.mu.Lock()
	ix.m[repo] = idx
	ix.mu.Unlock()
}

// readIndex loads one repo index; missing/corrupt yields nil.
func readIndex(ctx context.Context, st storeAPI, repo string) *Index {
	rc, _, err := st.Get(ctx, IndexKey(repo))
	if err != nil {
		return nil
	}
	defer rc.Close()
	var idx Index
	if err := json.NewDecoder(rc).Decode(&idx); err != nil || len(idx.Versions) == 0 {
		log.Printf("local index for %s not valid, treating as absent", repo)
		return nil
	}
	return &idx
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
func (ix *Index) Lookup(rev string) (Version, bool) {
	if len(ix.Versions) == 0 {
		return Version{}, false
	}
	if rev == "" || rev == "main" {
		// Versions are stored oldest-first; the last entry is latest-sealed.
		last := ix.Versions[len(ix.Versions)-1]
		return last, true
	}
	for _, v := range ix.Versions {
		if v.Version == rev || v.Commit == rev {
			return v, true
		}
	}
	return Version{}, false
}

// CommitOf derives the synthetic 40-hex commit from the seal content:
// sha256 over the canonical JSON of identity+files+sizes (files and sizes
// sorted by path), first 40 hex chars. Deterministic across restarts and
// re-seals of identical content, so clients can pin it safely.
func CommitOf(identity string, files map[string]string, sizes map[string]int64) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\n", identity)
	for _, f := range sortedKeys(files) {
		fmt.Fprintf(h, "%s %s %d\n", f, files[f], sizes[f])
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:20])
}

// sortedKeys returns files' keys sorted (map iteration order is random).
func sortedKeys(m map[string]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
