package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/manifest"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// chunkSize is the bounded buffer used while teeing an upstream body to the
// client and the store.
const chunkSize = 1 << 20 // 1 MiB

// fileRe is the allowed shape of a repo-relative file path: alphanumerics,
// dots, dashes, underscores and slashes (nested paths), no empty segments,
// no "..". See validFilePath.
var fileRe = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// RegisterFiles mounts the file resolve lane on mux.
//
// ROUTING CONTRACT: this pattern must NOT be registered on the same mux as
// the /api/models/ subtree. Go 1.22 ServeMux precedence is
// specificity-based, not registration-order-based: "/api/models/resolve/x/"
// matches both "/api/models/" and "/{org}/{name}/resolve/{rev}/{file...}"
// with neither pattern more specific, so co-registration panics at startup
// regardless of order. The caller mounts a child mux carrying this pattern
// under "/" on the main mux (see newMux in cmd/hf-cache-d and newTestServer),
// where the literal /healthz and /api/models/ routes win every overlap.
//
// Pattern shape note: a "{repo...}" multi-segment wildcard is invalid Go
// syntax anywhere but the end, and repo ids are exactly {org}/{name}, so
// the explicit two-segment form covers the same URL space.
func (p *Proxy) RegisterFiles(mux *http.ServeMux) {
	mux.HandleFunc("/{org}/{name}/resolve/{rev}/{file...}", p.handleResolveFile)
}

// handleResolveFile serves GET/HEAD /{org}/{name}/resolve/{rev}/{file...}.
//
// S3 key layout (documented contract):
//
//	pub/<sha[0:2]>/<sha>/<repo>/<file>   file body
//	pub/<sha[0:2]>/<sha>/manifest.json   release manifest (Files: path → sha256)
//
// A file counts as cached only when its manifest entry exists: an S3 object
// without a manifest entry is a partial or aborted upload and is never
// served. The manifest is authoritative on every request.
func (p *Proxy) handleResolveFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		p.writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	repo := r.PathValue("org") + "/" + r.PathValue("name")
	rev, file := r.PathValue("rev"), r.PathValue("file")

	if !validRepo(repo) {
		p.writeErr(w, http.StatusBadRequest, "invalid repo id: want {org}/{name}")
		return
	}
	if !validRev(rev) {
		p.writeErr(w, http.StatusBadRequest, "invalid revision")
		return
	}
	if !validFilePath(file) {
		p.writeErr(w, http.StatusBadRequest, "invalid file path")
		return
	}

	// Sealed local models shadow the public repo for every revision: check
	// FIRST, and never fall back to the public flow once sealed.
	if p.handleResolveFileLocal(w, r, repo, rev, file) {
		return
	}

	sha, err := p.resolve(r.Context(), repo, rev)
	if err != nil {
		p.fail(w, err)
		return
	}

	if sum, size, ok := p.fileEntry(r.Context(), sha, file); ok {
		key := fileKeyOf(sha, repo, file)
		p.serveHit(w, r, repo, sha, file, key, sum, size,
			func() { p.serveHeadMiss(w, r, repo, sha, file) },
			func() { p.serveGetMiss(w, r, repo, sha, file, key) })
		return
	}
	if r.Method == http.MethodHead {
		p.serveHeadMiss(w, r, repo, sha, file)
		return
	}
	p.serveGetMiss(w, r, repo, sha, file, fileKeyOf(sha, repo, file))
}

// fileKeyOf builds the body object key for repo@sha/file.
func fileKeyOf(sha, repo, file string) string {
	return fmt.Sprintf("pub/%s/%s/%s/%s", sha[:2], sha, repo, file)
}

// manifestKeyOf builds the manifest object key for a commit sha.
func manifestKeyOf(sha string) string {
	return fmt.Sprintf("pub/%s/%s/manifest.json", sha[:2], sha)
}

// validFilePath enforces the file-lane shape: [A-Za-z0-9._/-]+, no "..", no
// empty segments (leading "/", "//", trailing "/"), length-capped. HF file
// names may contain dots (".gitignore"), dashes, underscores and slashes
// for nested paths; anything outside that set is rejected as a 400.
func validFilePath(file string) bool {
	if file == "" || len(file) > 511 {
		return false
	}
	if !fileRe.MatchString(file) {
		return false
	}
	return !strings.Contains(file, "..") &&
		!strings.Contains(file, "//") &&
		!strings.HasPrefix(file, "/") &&
		!strings.HasSuffix(file, "/")
}

// ---- manifest access ----
//
// Lock discipline: manMu guards BOTH the manifests map and the Files/Sizes
// maps of every cached *Manifest (they are shared between readers). Reads
// take manMu for the map lookup AND the entry lookup. Publishers never
// mutate a cached manifest in place: they build the merged copy, Put it to
// the store, and only on success install it into the cache under manMu —
// a failed Put leaves the cache exactly as it was (no phantom entries).

// fileEntry looks up the manifest entry for repo@sha/file: its sha256 (our
// ETag) and byte size. Misses (no manifest, no entry, undecodable manifest)
// are not cached negatively: each cold request costs one manifest read.
// ponytail: single-process read cache; a second proxy instance would need
// to drop it (or add store-side versioning) to stay coherent.
func (p *Proxy) fileEntry(ctx context.Context, sha, file string) (sum string, size int64, ok bool) {
	p.manMu.Lock()
	m, cached := p.manifests[sha]
	if cached {
		defer p.manMu.Unlock()
		sum, ok = m.Files[file]
		if ok {
			return sum, m.Sizes[file], true
		}
		return "", 0, false
	}
	p.manMu.Unlock()

	// Cache miss: load from the store, then install without clobbering a
	// concurrently-installed newer entry.
	m = p.loadManifest(ctx, sha)
	if m == nil {
		return "", 0, false
	}
	m = p.installIfNewer(sha, m)
	p.manMu.Lock()
	defer p.manMu.Unlock()
	sum, ok = m.Files[file]
	if !ok {
		return "", 0, false
	}
	return sum, m.Sizes[file], true
}

// loadManifest reads manifest.json for sha from the store and returns it
// WITHOUT caching (the caller decides). A missing or corrupt manifest
// yields nil (self-heals on next publish). The S3 read happens outside
// manMu; installIfNewer below serializes the install against concurrent
// publishers so a stale read can never overwrite a newer cached manifest.
func (p *Proxy) loadManifest(ctx context.Context, sha string) *manifest.Manifest {
	rc, _, err := p.store.Get(ctx, manifestKeyOf(sha))
	if err != nil {
		return nil
	}
	defer rc.Close()
	var m manifest.Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil || m.Files == nil {
		log.Printf("manifest for %s not valid, treating as absent", sha)
		return nil
	}
	m.CacheKey = manifestKeyOf(sha)
	return &m
}

// installIfNewer caches m for sha unless a manifest with a superset of
// entries is already cached (a concurrent publish won the race). Returns
// the manifest the cache now holds.
func (p *Proxy) installIfNewer(sha string, m *manifest.Manifest) *manifest.Manifest {
	p.manMu.Lock()
	defer p.manMu.Unlock()
	if cur, ok := p.manifests[sha]; ok {
		if cur.CacheKey == "" {
			cur.CacheKey = m.CacheKey
		}
		for file := range m.Files {
			if _, exists := cur.Files[file]; !exists {
				// The store read knew a file the cache lacks: merge it in.
				// (Possible when another process wrote the manifest, or this
				// read raced a publish that has not installed yet.)
				cur.Files[file] = m.Files[file]
				if m.Sizes != nil {
					if cur.Sizes == nil {
						cur.Sizes = map[string]int64{}
					}
					cur.Sizes[file] = m.Sizes[file]
				}
			}
		}
		return cur
	}
	p.manifests[sha] = m
	return m
}

// publishFile merges file→sum (size) into the release manifest for sha.
// The per-release mutex serializes the store read-modify-write (the store
// has no CAS); manMu guards the cache install, which happens only after a
// successful store Put so a failed publish leaves no phantom entry.
func (p *Proxy) publishFile(ctx context.Context, repo, sha, file, sum string, size int64) {
	mu := p.muFor(sha)
	mu.Lock()
	defer mu.Unlock()

	m := p.loadManifest(ctx, sha)
	if m == nil {
		m = &manifest.Manifest{
			Identity: "hf:" + repo + "@" + sha,
			Files:    map[string]string{},
			Sizes:    map[string]int64{},
			PulledAt: time.Now(),
			Upstream: p.upstream,
		}
	}
	if _, exists := m.Files[file]; exists {
		return // a concurrent pull already published this path
	}
	// Build the merged copy: never mutate a shared cached manifest.
	next := manifest.Manifest{
		Identity: m.Identity,
		Files:    make(map[string]string, len(m.Files)+1),
		Sizes:    make(map[string]int64, len(m.Files)+1),
		PulledAt: m.PulledAt,
		Upstream: m.Upstream,
	}
	if next.Identity == "" {
		next.Identity = "hf:" + repo + "@" + sha
		next.PulledAt = time.Now()
		next.Upstream = p.upstream
	}
	for k, v := range m.Files {
		next.Files[k] = v
	}
	for k, v := range m.Sizes {
		next.Sizes[k] = v
	}
	next.Files[file] = sum
	next.Sizes[file] = size

	body, err := json.Marshal(next)
	if err != nil {
		log.Printf("manifest marshal %s: %v", sha, err)
		return
	}
	key := manifestKeyOf(sha)
	if err := p.store.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		log.Printf("manifest put %s: %v", key, err)
		return
	}
	next.CacheKey = key
	p.manMu.Lock()
	p.manifests[sha] = &next
	p.manMu.Unlock()
}

// muFor returns the in-process read-modify-write lock for one release.
func (p *Proxy) muFor(sha string) *sync.Mutex {
	p.relMu.Lock()
	defer p.relMu.Unlock()
	if mu, ok := p.releaseMu[sha]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	p.releaseMu[sha] = mu
	// ponytail: unbounded lock map, evict when release count grows.
	return mu
}

// ManifestKeys lists the store keys of every manifest held in the read
// cache (public release manifests and seal manifests alike). It feeds the
// cmd's integrity self-check, which needs candidate manifests to verify but
// must not list the bucket (the store has no List).
func (p *Proxy) ManifestKeys() []string {
	p.manMu.Lock()
	defer p.manMu.Unlock()
	keys := make([]string, 0, len(p.manifests))
	for _, m := range p.manifests {
		keys = append(keys, m.CacheKey)
	}
	return keys
}

// ---- serving: cache hit ----

// serveHit serves a manifest-backed object. The manifest is authoritative
// for what SHOULD exist, but store.Head gates existence on the HEAD and
// ranged paths: an object that vanished from the store while its manifest
// entry survives self-heals onto the miss path instead of a false HIT. (The
// full-GET path discovers the same through store.Get ErrNotFound.) Manifest
// Sizes back-fill the length only when Head reports none. onMiss/onGetMiss
// are the lane-specific miss continuations (upstream fetch for the public
// lane, 404 for local models — a sealed local file has no upstream).
func (p *Proxy) serveHit(w http.ResponseWriter, r *http.Request, repo, sha, file, key, sum string, size int64, onMiss, onGetMiss func()) {
	p.m.AddHit()
	h := w.Header()
	h.Set("X-Repo-Commit", sha)
	h.Set("X-Cache", "HIT")
	h.Set("Accept-Ranges", "bytes")
	h.Set("ETag", `"`+sum+`"`)

	if r.Method == http.MethodHead {
		exists, hsize, err := p.store.Head(r.Context(), key)
		if err != nil {
			log.Printf("store head %s: %v", key, err)
			p.writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		if !exists {
			onMiss()
			return
		}
		if hsize > 0 {
			size = hsize
		}
		h.Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		return
	}

	if rg := r.Header.Get("Range"); rg != "" {
		exists, hsize, err := p.store.Head(r.Context(), key)
		if err != nil {
			log.Printf("store head %s: %v", key, err)
			p.writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		if !exists {
			onGetMiss()
			return
		}
		if hsize > 0 {
			size = hsize
		}
		if start, end, ok := parseByteRange(rg, size); ok {
			if start < 0 {
				h.Set("Content-Range", fmt.Sprintf("bytes */%d", size))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			rc, err := p.store.GetRange(r.Context(), key, start, end+1)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					// Vanished between Head and GetRange: self-heal.
					onGetMiss()
					return
				}
				log.Printf("store get range %s: %v", key, err)
				p.writeErr(w, http.StatusInternalServerError, "internal error")
				return
			}
			defer rc.Close()
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
			h.Set("Content-Length", strconv.FormatInt(end-start+1, 10))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.Copy(w, rc)
			return
		}
		// Malformed/multi/suffix range: ignore, serve the full body below.
	}

	rc, size, err := p.store.Get(r.Context(), key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			onGetMiss()
			return
		}
		log.Printf("store get %s: %v", key, err)
		p.writeErr(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer rc.Close()
	h.Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// parseByteRange parses a single "bytes=a-b" Range header against size.
// ok=false means no usable range: absent, malformed, multi ("a-b,c-d") or
// suffix ("-n") — the caller serves a full 200, matching HF CDN behavior
// for multi-range. start=-1 with ok=true means unsatisfiable (start >=
// size): the caller serves 416. end is clamped to size-1.
func parseByteRange(hdr string, size int64) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(hdr, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, false
	}
	a, b, found := strings.Cut(spec, "-")
	if !found || a == "" {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if b == "" {
		end = size - 1
	} else if end, err = strconv.ParseInt(b, 10, 64); err != nil || end < start {
		return 0, 0, false
	}
	if start >= size {
		return -1, 0, true
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

// ---- serving: HEAD miss ----

// serveHeadMiss answers a cold HEAD from a lightweight upstream HEAD
// against the SHA-pinned resolve URL. The shared client follows HF's 302 to
// the CDN while preserving the HEAD method, so the final response carries
// X-Linked-ETag / X-Linked-Size (or plain ETag/Content-Length). Nothing is
// cached: HEAD never populates the cache, or HEAD-only objects would mint
// manifest-less entries.
func (p *Proxy) serveHeadMiss(w http.ResponseWriter, r *http.Request, repo, sha, file string) {
	p.m.AddMiss()
	resp, err := p.fetchFile(r.Context(), http.MethodHead, resolvePath(repo, sha, file), "")
	if err != nil {
		p.fail(w, err)
		return
	}
	defer resp.Body.Close()

	h := w.Header()
	h.Set("X-Repo-Commit", sha)
	h.Set("X-Cache", "MISS")
	h.Set("Accept-Ranges", "bytes")
	if v := firstNonEmpty(resp.Header.Get("X-Linked-ETag"), resp.Header.Get("ETag")); v != "" {
		h.Set("ETag", v)
	}
	if v := firstNonEmpty(resp.Header.Get("X-Linked-Size"), resp.Header.Get("Content-Length")); v != "" {
		h.Set("Content-Length", v)
	}
	if v := resp.Header.Get("Content-Type"); v != "" {
		h.Set("Content-Type", v)
	}
	w.WriteHeader(resp.StatusCode)
}

func resolvePath(repo, sha, file string) string {
	return fmt.Sprintf("/%s/resolve/%s/%s", repo, sha, file)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ---- serving: GET miss (the heart of the mirror) ----

// serveGetMiss pulls the file from upstream (the shared client follows the
// CDN redirect server-side; the Location never reaches ours), tees the body
// chunk-wise to the client and into a streaming S3 upload while hashing,
// and publishes the manifest entry only after upstream EOF plus a
// successful store Put. Any failure mid-flight aborts the upload and
// publishes nothing: an object without a manifest entry is never served.
//
// The upstream fetch, upload and publish run under a context detached from
// the client request, so a client disconnect stops only the client relay;
// the server-side download runs to completion.
// ponytail: unlimited detached cold pulls — add a semaphore and timeout if
// this is ever exposed to untrusted networks.
func (p *Proxy) serveGetMiss(w http.ResponseWriter, r *http.Request, repo, sha, file, key string) {
	// Cold ranged GET: relay upstream's answer without caching.
	// ponytail: partial responses never populate the cache; the file warms
	// on its next full GET. Acceptable: HF clients rarely range-cold-pull.
	if rg := r.Header.Get("Range"); rg != "" {
		p.relayColdRange(w, r, repo, sha, file, rg)
		return
	}

	ctx := context.Background() // detached: see comment above
	p.m.AddMiss()
	resp, err := p.fetchFile(ctx, http.MethodGet, resolvePath(repo, sha, file), "")
	if err != nil {
		p.fail(w, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		p.relayStatus(w, resp, sha)
		return
	}

	// Tee: every chunk read upstream goes to the client, the hash, and the
	// pipe feeding the store upload (multipart streaming, unknown size).
	pr, pw := io.Pipe()
	putDone := make(chan error, 1)
	go func() {
		err := p.store.Put(ctx, key, pr, -1)
		if err != nil {
			// Unblock the tee loop's pipe writes if the upload aborted early.
			pr.CloseWithError(err)
		}
		putDone <- err
	}()

	h := w.Header()
	h.Set("X-Repo-Commit", sha)
	h.Set("X-Cache", "MISS")
	h.Set("Accept-Ranges", "bytes")
	if v := resp.Header.Get("Content-Type"); v != "" {
		h.Set("Content-Type", v)
	}
	if v := resp.Header.Get("Content-Length"); v != "" {
		h.Set("Content-Length", v)
	}
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	hash := sha256.New()
	buf := make([]byte, chunkSize)
	clientGone, storeGone := false, false
	var relayed int64
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			relayed += int64(n)
			hash.Write(buf[:n])
			if !clientGone {
				if _, werr := w.Write(buf[:n]); werr != nil {
					clientGone = true
					log.Printf("client relay %s/%s stopped after %d bytes: %v (download continues)", repo, file, relayed, werr)
				} else if flusher != nil {
					flusher.Flush()
				}
			}
			if !storeGone {
				if _, perr := pw.Write(buf[:n]); perr != nil {
					storeGone = true
					log.Printf("store ingest %s stopped: %v (serving continues, not publishing)", key, perr)
				}
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// Upstream truncated or errored mid-body. The client already
			// received the partial bytes (accepted v1 behavior); abort the
			// upload and publish nothing so the next request re-fetches.
			pw.CloseWithError(fmt.Errorf("upstream read: %w", rerr))
			log.Printf("upstream read %s/%s after %d bytes: %v (not published)", repo, file, relayed, rerr)
			return
		}
	}
	pw.Close()                  // EOF: let the upload finish
	p.m.AddBytesPulled(relayed) // full-body ingest only: ranged relays store nothing

	if perr := <-putDone; perr != nil {
		// Client got the full body; the partial object is garbage and stays
		// unpublished. Task 6's integrity pass covers retention.
		log.Printf("store put %s: %v (object is garbage, not published)", key, perr)
		return
	}
	p.publishFile(ctx, repo, sha, file, hex.EncodeToString(hash.Sum(nil)), relayed)
}

// relayStatus relays a non-200 upstream GET answer (e.g. a 304 or 404 that
// slipped past the status mapping) verbatim.
func (p *Proxy) relayStatus(w http.ResponseWriter, resp *http.Response, sha string) {
	p.m.AddMiss()
	h := w.Header()
	h.Set("X-Repo-Commit", sha)
	h.Set("X-Cache", "MISS")
	for _, k := range []string{"Content-Type", "Content-Length", "Content-Range", "ETag"} {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// relayColdRange relays a cold ranged GET to upstream with the client's
// Range header and caches nothing: partial responses never populate the
// cache, so the file stays a MISS until a full GET happens.
func (p *Proxy) relayColdRange(w http.ResponseWriter, r *http.Request, repo, sha, file, rg string) {
	p.m.AddMiss()
	resp, err := p.fetchFile(r.Context(), http.MethodGet, resolvePath(repo, sha, file), rg)
	if err != nil {
		p.fail(w, err)
		return
	}
	defer resp.Body.Close()
	h := w.Header()
	h.Set("X-Repo-Commit", sha)
	h.Set("X-Cache", "MISS")
	for _, k := range []string{"Content-Type", "Content-Length", "Content-Range", "ETag", "Accept-Ranges"} {
		if v := resp.Header.Get(k); v != "" {
			h.Set(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// fetchFile is the file lane's retrying fetch: one retry on connection
// errors and 5xx (mirroring the metadata lane's policy). Retries happen
// only before any body byte is relayed, so a streaming GET never restarts
// mid-flight.
func (p *Proxy) fetchFile(ctx context.Context, method, path, rangeHdr string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		resp, err := p.fetchResp(ctx, method, path, rangeHdr)
		if err == nil {
			return resp, nil
		}
		if attempt == 1 || !retriable(err) {
			return nil, err
		}
	}
}

// fetchResp performs one anonymous upstream request for the file lane.
// 404 maps to a client-facing 404; other 4xx and 5xx map to 502 with the
// real status recorded for the retry policy. The redirect to the CDN is
// followed server-side by the shared client; its Location is never copied
// onto our response.
func (p *Proxy) fetchResp(ctx context.Context, method, path, rangeHdr string) (*http.Response, error) {
	u := p.upstream + path
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	// Anonymous: inbound Authorization/Cookie never reach upstream. The
	// file client has no overall deadline so detached transfers run to
	// completion (see the Proxy.fileClient comment).
	resp, err := p.fileClient.Do(req)
	if err != nil {
		return nil, &upstreamError{status: http.StatusBadGateway, msg: "upstream unreachable"}
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		resp.Body.Close()
		return nil, &upstreamError{status: http.StatusNotFound, upstream: resp.StatusCode, msg: "not found upstream"}
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && rangeHdr != "":
		// A cold ranged GET whose range upstream rejects must reach the
		// client as upstream's own 416, not be re-mapped to a 502.
		return resp, nil
	case resp.StatusCode >= 400:
		resp.Body.Close()
		return nil, &upstreamError{status: http.StatusBadGateway, upstream: resp.StatusCode, msg: "upstream error"}
	}
	return resp, nil
}
