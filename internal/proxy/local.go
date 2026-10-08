// local.go integrates sealed private models (push lane) into the
// HF-compatible read lanes. A repo with at least one seal SHADOWS the public
// repo of the same name for every revision: requests that match a local
// version or synthetic commit serve from the sealed snapshot; anything else
// (including "main" when no seal exists yet, or a 40-hex public SHA) is a
// 404. There is NEVER a fallback to the public flow once sealed — that is
// the user decision this file implements.
package proxy

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/manifest"
)

// SetLocalIndexes wires the sealed-model index cache into the proxy. It is
// set once at construction time by the caller that owns both lanes (main.go
// or tests); nil (the zero state) means no local models exist.
func (p *Proxy) SetLocalIndexes(ix *local.Indexes) { p.locals = ix }

// localVersion returns the sealed version repo@rev resolves to, and whether
// the repo is local at all. A local repo with no matching rev yields
// (zero, true): the caller must 404 (no public fallback). A store failure
// yields ok=false with served=true: the caller has already written a 503
// (again never the public flow).
func (p *Proxy) localVersion(w http.ResponseWriter, r *http.Request, repo, rev string) (v local.Version, isLocal, served bool) {
	if p.locals == nil {
		return local.Version{}, false, false
	}
	idx, err := p.locals.Get(r.Context(), repo)
	if err != nil {
		log.Printf("local index for %s: %v", repo, err)
		p.writeErr(w, http.StatusServiceUnavailable, "local index unavailable")
		return local.Version{}, false, true
	}
	if idx == nil {
		return local.Version{}, false, false
	}
	v, _ = idx.Lookup(rev)
	return v, true, false
}

// localManifest loads the seal manifest for repo@version through the shared
// manifest read cache (same lock discipline as the public lane: the cached
// *Manifest's maps are read under manMu). A store failure is an error —
// never a silent nil, which the caller would render as "missing" and the
// shadowing rule would turn into a wrong answer.
func (p *Proxy) localManifest(r *http.Request, repo, version string) (*manifest.Manifest, error) {
	p.manMu.Lock()
	if m, ok := p.manifests[sealKey(repo, version)]; ok {
		p.manMu.Unlock()
		return m, nil
	}
	p.manMu.Unlock()

	rc, _, err := p.store.Get(r.Context(), sealKey(repo, version))
	if err != nil {
		return nil, fmt.Errorf("seal manifest read: %w", err)
	}
	defer rc.Close()
	var m manifest.Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil || m.Files == nil {
		log.Printf("seal manifest for %s@%s not valid, treating as missing", repo, version)
		return nil, nil
	}
	p.manMu.Lock()
	if cur, ok := p.manifests[sealKey(repo, version)]; ok {
		p.manMu.Unlock()
		return cur, nil
	}
	p.manifests[sealKey(repo, version)] = &m
	p.manMu.Unlock()
	return &m, nil
}

// handleModelsLocal serves the metadata lane (info/tree) for a sealed model.
// Returns false when the repo is not local and the public flow must run.
func (p *Proxy) handleModelsLocal(w http.ResponseWriter, r *http.Request, repo, kind, rev string) bool {
	v, isLocal, served := p.localVersion(w, r, repo, rev)
	if served {
		return true // 503 already written; never the public flow
	}
	if !isLocal {
		return false
	}
	if v.Commit == "" {
		p.writeErr(w, http.StatusNotFound, "no such revision (local model)")
		return true
	}
	m, err := p.localManifest(r, repo, v.Version)
	if err != nil {
		log.Printf("local model %s@%s: %v", repo, v.Version, err)
		p.writeErr(w, http.StatusServiceUnavailable, "sealed model unavailable")
		return true
	}
	if m == nil {
		p.writeErr(w, http.StatusInternalServerError, "sealed manifest missing")
		return true
	}

	var body []byte
	if kind == "tree" {
		body = p.localTree(m)
	} else {
		body = p.localInfo(repo, v, m)
	}
	// Local models are served from the store: always a HIT; the cache-status
	// header contract (MISS fetches upstream) has no miss to report.
	p.writeBody(w, r, body, "HIT", v.Commit, "")
	return true
}

// localInfo synthesizes the HF repo-info shape for a sealed version.
func (p *Proxy) localInfo(repo string, v local.Version, m *manifest.Manifest) []byte {
	type sibling struct {
		Rfilename string `json:"rfilename"`
	}
	sibs := make([]sibling, 0, len(m.Files))
	for _, f := range sortedFilePaths(m) {
		sibs = append(sibs, sibling{Rfilename: f})
	}
	out, _ := json.Marshal(map[string]any{
		"id":       repo,
		"sha":      v.Commit,
		"private":  true,
		"siblings": sibs,
	})
	return out
}

// localTree synthesizes the HF tree listing (recursive, single page) from
// the manifest's Files map.
func (p *Proxy) localTree(m *manifest.Manifest) []byte {
	type entry struct {
		Type string `json:"type"`
		Path string `json:"path"`
		Size int64  `json:"size"` // always present: 0-byte files are legal
	}
	seen := map[string]bool{}
	out := []entry{}
	for _, f := range sortedFilePaths(m) {
		// Emit intermediate directories once, like HF's recursive tree.
		for dir := range dirsOf(f) {
			if !seen[dir] {
				seen[dir] = true
				out = append(out, entry{Type: "directory", Path: dir})
			}
		}
		out = append(out, entry{Type: "file", Path: f, Size: m.Sizes[f]})
	}
	b, _ := json.Marshal(out)
	return b
}

// handleResolveFileLocal serves the file lane for a sealed model. Returns
// false when the repo is not local and the public flow must run.
func (p *Proxy) handleResolveFileLocal(w http.ResponseWriter, r *http.Request, repo, rev, file string) bool {
	v, isLocal, served := p.localVersion(w, r, repo, rev)
	if served {
		return true // 503 already written; never the public flow
	}
	if !isLocal {
		return false
	}
	if v.Commit == "" {
		p.writeErr(w, http.StatusNotFound, "no such revision (local model)")
		return true
	}
	m, err := p.localManifest(r, repo, v.Version)
	if err != nil {
		log.Printf("local model %s@%s: %v", repo, v.Version, err)
		p.writeErr(w, http.StatusServiceUnavailable, "sealed model unavailable")
		return true
	}
	if m == nil {
		p.writeErr(w, http.StatusInternalServerError, "sealed manifest missing")
		return true
	}
	sum, ok := m.Files[file]
	if !ok {
		// A sealed snapshot is complete and immutable: a missing file is a
		// plain 404, never an upstream fetch (no public fallback).
		p.writeErr(w, http.StatusNotFound, "no such file in this revision")
		return true
	}
	p.serveHit(w, r, repo, v.Commit, file, local.FileKey(repo, v.Version, file), sum, m.Sizes[file],
		func() { p.writeErr(w, http.StatusNotFound, "sealed object missing from store") },
		func() { p.writeErr(w, http.StatusNotFound, "sealed object missing from store") })
	return true
}

// sortedFilePaths lists the manifest's file paths sorted, under manMu.
func sortedFilePaths(m *manifest.Manifest) []string {
	// Caller holds no lock here; the manifest was freshly loaded or cached
	// by localManifest, and sealed manifests are immutable after seal — no
	// concurrent mutation is possible (unlike public publishFile merges).
	ks := make([]string, 0, len(m.Files))
	for k := range m.Files {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// dirsOf returns every intermediate directory of a repo-relative path.
func dirsOf(file string) map[string]string {
	out := map[string]string{}
	parts := strings.Split(file, "/")
	for i := 1; i < len(parts); i++ {
		out[strings.Join(parts[:i], "/")] = ""
	}
	return out
}

// sealKey is the cache key for a seal manifest (its S3 key).
func sealKey(repo, version string) string { return local.ManifestKey(repo, version) }
