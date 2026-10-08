// Package push serves the authenticated private artifact lane: streaming
// PUT staging with hashing, idempotent-rejecting sealing into an immutable
// manifest, the durable per-repo index update, and the anonymous version
// listing. Reads of sealed artifacts go through the proxy's HF-compatible
// routes (internal/proxy/local.go); this package only writes and lists.
package push

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/manifest"
	"github.com/miadabdi/hf-cache-d/internal/metrics"
	"github.com/miadabdi/hf-cache-d/internal/store"
)

// manifestMaxBytes caps the seal request body (small JSON: path→sha map).
const manifestMaxBytes = 1 << 20 // 1 MiB

// versionRe is the allowed version shape: optional leading v, then a digit,
// then alphanumerics, dots, underscores, dashes; or a plain 64-hex digest.
var (
	versionRe = regexp.MustCompile(`^v?[0-9][A-Za-z0-9._-]*$`)
	hex64Re   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// Store is the store contract push consumes (same shape as proxy's storeAPI,
// exported so cmd's mux wiring can union it).
type Store interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Head(ctx context.Context, key string) (bool, int64, error)
}

// Lane is the private push lane.
type Lane struct {
	token string // empty = lane disabled (404 on PUT/POST)
	store Store
	index *local.Indexes
	m     *metrics.Counters // bytes-pulled counter for staged uploads

	mu    sync.Mutex
	locks map[string]*sync.Mutex // "org/name@version" -> staging/seal lock
	// ponytail: unbounded lock map, evict when repo count grows.

	idxMu    sync.Mutex
	repoLock map[string]*sync.Mutex // "org/name" -> index read-modify-write lock
	// ponytail: unbounded lock map, evict when repo count grows.
}

// New builds the push lane. Empty token disables PUT/POST (404).
func New(token string, st Store, ix *local.Indexes) *Lane {
	return &Lane{
		token:    token,
		store:    st,
		index:    ix,
		locks:    map[string]*sync.Mutex{},
		repoLock: map[string]*sync.Mutex{},
		m:        metrics.Default,
	}
}

// SetCounters installs a private counter set (tests).
func (l *Lane) SetCounters(c *metrics.Counters) { l.m = c }

// Register mounts the push routes on mux (the parent mux; /v1/artifacts/ is
// a literal subtree that wins over the file lane's "/").
func (l *Lane) Register(mux *http.ServeMux) {
	mux.HandleFunc("/v1/artifacts/", l.handle)
}

// handle routes /v1/artifacts/{org}/{name}[/{version}[/{file...}]].
func (l *Lane) handle(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/artifacts/")

	// The listing route: GET /v1/artifacts/{org}/{name} — anonymous.
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if repo, tail := cutRepo(rest); tail == "" && validRepo(repo) {
			l.handleList(w, r, repo)
			return
		}
	}

	// Everything else is a write: PUT stage or POST seal. Auth first.
	if l.token == "" {
		writeErr(w, http.StatusNotFound, "push lane disabled")
		return
	}
	if !l.authorized(r) {
		writeErr(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}

	repo, tail := cutRepo(rest)
	if !validRepo(repo) {
		writeErr(w, http.StatusBadRequest, "invalid repo id: want {org}/{name}")
		return
	}
	version, file, _ := strings.Cut(tail, "/")
	if !validVersion(version) {
		writeErr(w, http.StatusBadRequest, "invalid version")
		return
	}
	vkey := repo + "@" + version

	switch {
	case r.Method == http.MethodPost && file == "manifest":
		l.handleSeal(w, r, repo, version, vkey)
	case r.Method == http.MethodPut && file != "":
		l.handleStage(w, r, repo, version, vkey, file)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, POST")
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// authorized compares the bearer token in constant time.
func (l *Lane) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) <= len(prefix) || h[:len(prefix)] != prefix {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(h[len(prefix):]), []byte(l.token)) == 1
}

// handleStage streams the request body into the staging key while hashing,
// rejecting sealed versions (409) and overwrites of staged content (lazy:
// last PUT wins, matching object-store semantics).
func (l *Lane) handleStage(w http.ResponseWriter, r *http.Request, repo, version, vkey, file string) {
	if !validFilePath(file) {
		writeErr(w, http.StatusBadRequest, "invalid file path")
		return
	}
	if r.ContentLength < 0 {
		writeErr(w, http.StatusLengthRequired, "content-length required")
		return
	}

	mu := l.muFor(vkey)
	mu.Lock()
	defer mu.Unlock()

	sealed, err := l.isSealed(r.Context(), repo, version)
	if err != nil {
		log.Printf("push head %s: %v", local.ManifestKey(repo, version), err)
		writeErr(w, http.StatusInternalServerError, "store read failed")
		return
	}
	if sealed {
		writeErr(w, http.StatusConflict, "version is sealed and immutable")
		return
	}

	key := local.FileKey(repo, version, file)
	// Stream body → hash + S3 through a pipe: the SDK's single-shot Put
	// needs a seekable body (payload checksum), but the request body is not
	// seekable, so use the multipart streaming path like the file lane's
	// tee-through upload. The store goroutine closes the pipe reader on ALL
	// exits — including a Put that returns nil early WITHOUT draining the
	// body — so the handler's writes always surface (EPIPE), never block.
	pr, pw := io.Pipe()
	hash := sha256.New()
	putDone := make(chan error, 1)
	go func() {
		var perr error
		defer func() {
			pr.CloseWithError(perr) // nil error still closes: writers unblock
			putDone <- perr
		}()
		perr = l.store.Put(r.Context(), key, pr, -1)
	}()
	if _, err := io.Copy(io.MultiWriter(pw, hash), io.LimitReader(r.Body, r.ContentLength)); err != nil {
		pw.CloseWithError(err)
		<-putDone
		log.Printf("push read %s: %v", key, err)
		writeErr(w, http.StatusInternalServerError, "upload aborted")
		return
	}
	pw.Close()
	if err := <-putDone; err != nil {
		log.Printf("push put %s: %v", key, err)
		writeErr(w, http.StatusInternalServerError, "store write failed")
		return
	}
	if n, _ := io.Copy(io.Discard, r.Body); n > 0 {
		// Extra bytes beyond Content-Length: refuse rather than silently drop.
		writeErr(w, http.StatusBadRequest, "body longer than content-length")
		return
	}
	// Register only successful uploads. An inventory write failure returns
	// 500; the body is unreferenced and cannot be sealed as a completed PUT.
	stageKey := stageListKey(repo, version)
	staged, err := l.readStageList(r.Context(), stageKey)
	if err != nil {
		log.Printf("push stage list %s: %v", stageKey, err)
		writeErr(w, http.StatusInternalServerError, "stage list read failed")
		return
	}
	staged.Files[file] = true
	stageBody, _ := json.Marshal(staged)
	if err := l.store.Put(r.Context(), stageKey, strings.NewReader(string(stageBody)), int64(len(stageBody))); err != nil {
		log.Printf("push stage list put %s: %v", stageKey, err)
		writeErr(w, http.StatusInternalServerError, "stage list write failed")
		return
	}
	sum := hex.EncodeToString(hash.Sum(nil))
	l.m.AddBytesPulled(r.ContentLength)
	writeJSON(w, http.StatusCreated, map[string]string{
		"file":   file,
		"sha256": sum,
		"size":   fmt.Sprint(r.ContentLength),
	})
}

// sealReq is the POST manifest body.
type sealReq struct {
	Files map[string]string `json:"files"`
	Sizes map[string]int64  `json:"sizes"`
}

// handleSeal validates every listed file against the staged bytes (hashing
// the stored object — the client's hash is a claim, not truth), writes
// manifest.json last, then the repo index. A seal failure after the manifest
// write leaves a "sealed but unindexed" state; re-POST repairs it via
// recovery: the index gains the version if it is missing, and 200s.
func (l *Lane) handleSeal(w http.ResponseWriter, r *http.Request, repo, version, vkey string) {
	// Bound the RAW body, not just the decoder input: a small JSON document
	// followed by megabytes of trailing whitespace must not slip through.
	var req sealReq
	body, err := io.ReadAll(io.LimitReader(r.Body, manifestMaxBytes+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "manifest body unreadable")
		return
	}
	if len(body) > manifestMaxBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "manifest body exceeds 1MiB")
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "manifest body must be JSON {files:{path:sha256},sizes:{path:n}}")
		return
	}
	if len(req.Files) == 0 {
		writeErr(w, http.StatusBadRequest, "manifest must list at least one file")
		return
	}

	mu := l.muFor(vkey)
	mu.Lock()
	defer mu.Unlock()

	idx, err := l.index.Get(r.Context(), repo)
	if err != nil {
		log.Printf("local index for %s: %v", repo, err)
		writeErr(w, http.StatusInternalServerError, "index read failed")
		return
	}
	if idx != nil {
		if v, ok := idx.Lookup(version); ok && v.Commit != "" {
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":  "version already sealed",
				"commit": v.Commit,
			})
			return
		}
	}

	stageKey := stageListKey(repo, version)
	staged, err := l.readStageList(r.Context(), stageKey)
	if err != nil {
		log.Printf("push stage list %s: %v", stageKey, err)
		writeErr(w, http.StatusInternalServerError, "stage list read failed")
		return
	}
	for file := range req.Files {
		if !staged.Files[file] {
			writeErr(w, http.StatusUnprocessableEntity, fmt.Sprintf("manifest file %q was not staged", file))
			return
		}
	}
	for file := range staged.Files {
		if _, ok := req.Files[file]; !ok {
			writeErr(w, http.StatusUnprocessableEntity, fmt.Sprintf("staged file %q omitted from manifest", file))
			return
		}
	}

	// Validate the staged bytes: head every object, hash it, compare. The
	// stored hash is truth; a client-supplied hash that disagrees is 422.
	sizes := map[string]int64{}
	for path, want := range req.Files {
		if !validFilePath(path) || !hex64Re.MatchString(want) {
			writeErr(w, http.StatusBadRequest, fmt.Sprintf("invalid file entry %q", path))
			return
		}
		key := local.FileKey(repo, version, path)
		exists, _, err := l.store.Head(r.Context(), key)
		if err != nil {
			log.Printf("push head %s: %v", key, err)
			writeErr(w, http.StatusInternalServerError, "store read failed")
			return
		}
		if !exists {
			writeErr(w, http.StatusNotFound, fmt.Sprintf("staged file %q not found", path))
			return
		}
		rc, _, err := l.store.Get(r.Context(), key)
		if err != nil {
			log.Printf("push get %s: %v", key, err)
			writeErr(w, http.StatusInternalServerError, "store read failed")
			return
		}
		got, size, err := manifest.HashReader(rc)
		rc.Close()
		if err != nil {
			log.Printf("push hash %s: %v", key, err)
			writeErr(w, http.StatusInternalServerError, "store read failed")
			return
		}
		if got != want {
			writeErr(w, http.StatusUnprocessableEntity, fmt.Sprintf("sha256 mismatch for %q: staged bytes hash to %s", path, got))
			return
		}
		if listed, ok := req.Sizes[path]; ok && listed != size {
			writeErr(w, http.StatusUnprocessableEntity, fmt.Sprintf("size mismatch for %q: staged object is %d bytes", path, size))
			return
		}
		sizes[path] = size
	}

	identity := "private:" + repo + "@" + version
	commit := local.CommitOf(identity, req.Files, sizes)
	mkey := local.ManifestKey(repo, version)

	// Immutability + recovery gate: if a manifest object already exists,
	// this version was sealed before (possibly with a failed index write).
	// Identical content (same derived commit) continues to the index update
	// — that IS the recovery; different content is an immutable conflict.
	prev, err := l.readManifest(r.Context(), repo, version)
	if err != nil {
		log.Printf("push manifest read %s: %v", mkey, err)
		writeErr(w, http.StatusInternalServerError, "manifest read failed")
		return
	}
	if prev != nil {
		if local.CommitOf(prev.Identity, prev.Files, prev.Sizes) != commit {
			writeErr(w, http.StatusConflict, "version is sealed and immutable")
			return
		}
	} else {
		m := manifest.Manifest{
			Identity: identity,
			Files:    req.Files,
			Sizes:    sizes,
			PulledAt: time.Now(),
		}
		body, err := json.Marshal(m)
		if err != nil {
			log.Printf("push manifest marshal: %v", err)
			writeErr(w, http.StatusInternalServerError, "internal error")
			return
		}
		if err := l.store.Put(r.Context(), mkey, strings.NewReader(string(body)), int64(len(body))); err != nil {
			log.Printf("push put %s: %v", mkey, err)
			writeErr(w, http.StatusInternalServerError, "seal failed before index update")
			return
		}
	}

	// Index update: append + move main, serialized repo-wide so concurrent
	// seals of different versions cannot lose each other's entries — the
	// store read-modify-write AND the in-process cache install happen inside
	// one critical section, so a later-finishing seal can never install a
	// staler index than one already installed. A failure here is reported
	// (500), never claimed complete; recovery is re-POST (see above).
	if err := l.commitIndex(r.Context(), repo, version, commit); err != nil {
		writeErr(w, http.StatusInternalServerError, "sealed but index update failed; re-post manifest to recover")
		return
	}

	// Mark sealed, retaining inventory for recovery instead of requiring a
	// store Delete operation. Sealed manifests remain the immutability gate.
	staged.Sealed = true
	stageBody, _ := json.Marshal(staged)
	if err := l.store.Put(r.Context(), stageKey, strings.NewReader(string(stageBody)), int64(len(stageBody))); err != nil {
		log.Printf("push stage list mark sealed %s: %v", stageKey, err)
		// The seal and index are durable; do not report a false failure.
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"version": version,
		"commit":  commit,
	})
}

// readManifest returns nil only for a confirmed absent seal manifest.
func (l *Lane) readManifest(ctx context.Context, repo, version string) (*manifest.Manifest, error) {
	rc, _, err := l.store.Get(ctx, local.ManifestKey(repo, version))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var m manifest.Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, err
	}
	if m.Files == nil {
		return nil, fmt.Errorf("invalid seal manifest: missing files")
	}
	return &m, nil
}

// stage.json tracks the exact set of successfully staged paths. No bucket
// listing or new store API is needed; the existing version lock serializes it.
type stageList struct {
	Files  map[string]bool `json:"files"`
	Sealed bool            `json:"sealed,omitempty"`
}

func stageListKey(repo, version string) string {
	return "priv/" + repo + "/" + version + "/stage.json"
}

func (l *Lane) readStageList(ctx context.Context, key string) (stageList, error) {
	rc, _, err := l.store.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return stageList{Files: map[string]bool{}}, nil
	}
	if err != nil {
		return stageList{}, err
	}
	defer rc.Close()
	var staged stageList
	if err := json.NewDecoder(rc).Decode(&staged); err != nil {
		return stageList{}, err
	}
	if staged.Files == nil {
		return stageList{}, fmt.Errorf("invalid stage list: missing files")
	}
	return staged, nil
}

// isSealed reports whether the version's manifest object exists.
func (l *Lane) isSealed(ctx context.Context, repo, version string) (bool, error) {
	exists, _, err := l.store.Head(ctx, local.ManifestKey(repo, version))
	return exists, err
}

// commitIndex performs the durable index update for a seal: under the
// repo-level index lock, read the index from the store, append the newly
// sealed version, write it back, and install the result in the in-process
// cache — all inside the SAME critical section. Lock ordering matters: a
// seal whose store write completes late still installs under the lock, so
// it reads (and therefore extends) whatever the earlier finisher wrote and
// the cached view can never regress to a staler index.
func (l *Lane) commitIndex(ctx context.Context, repo, version, commit string) error {
	mu := l.muForRepo(repo)
	mu.Lock()
	defer mu.Unlock()

	idx, err := readStoreIndex(ctx, l.store, repo)
	if err != nil {
		return fmt.Errorf("index read %s: %w", repo, err)
	}
	if idx == nil {
		idx = &local.Index{}
	}
	// Seal-time ordering: entries carry SealedAt and the list is rebuilt
	// oldest-first so a clock anomaly between seals cannot break "main".
	for i, v := range idx.Versions {
		if v.Version == version {
			idx.Versions = append(idx.Versions[:i], idx.Versions[i+1:]...)
			break
		}
	}
	idx.Versions = append(idx.Versions, local.Version{
		Version:  version,
		Commit:   commit,
		SealedAt: time.Now(),
	})
	for i := 1; i < len(idx.Versions); i++ {
		for j := i; j > 0 && idx.Versions[j].SealedAt.Before(idx.Versions[j-1].SealedAt); j-- {
			idx.Versions[j], idx.Versions[j-1] = idx.Versions[j-1], idx.Versions[j]
		}
	}
	idx.Main = idx.Versions[len(idx.Versions)-1].Commit

	body, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	ikey := local.IndexKey(repo)
	if err := l.store.Put(ctx, ikey, strings.NewReader(string(body)), int64(len(body))); err != nil {
		return err
	}
	l.index.Refresh(repo, idx) // inside the lock: see commitIndex doc
	return nil
}

// readStoreIndex re-reads the repo index from the store (bypasses the cache:
// the cache may be stale relative to a recovery).
func readStoreIndex(ctx context.Context, st Store, repo string) (*local.Index, error) {
	rc, _, err := st.Get(ctx, local.IndexKey(repo))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var idx local.Index
	if err := json.NewDecoder(rc).Decode(&idx); err != nil {
		return nil, err
	}
	if idx.Versions == nil {
		return nil, fmt.Errorf("invalid index: missing versions")
	}
	return &idx, nil
}

// handleList serves the anonymous version listing.
func (l *Lane) handleList(w http.ResponseWriter, r *http.Request, repo string) {
	idx, err := l.index.Get(r.Context(), repo)
	if err != nil {
		log.Printf("local index for %s: %v", repo, err)
		writeErr(w, http.StatusInternalServerError, "index read failed")
		return
	}
	if idx == nil {
		writeErr(w, http.StatusNotFound, "unknown repo")
		return
	}
	type listed struct {
		Version  string    `json:"version"`
		Commit   string    `json:"commit"`
		SealedAt time.Time `json:"sealedAt"`
		Files    int       `json:"files"`
	}
	out := struct {
		Repo     string   `json:"repo"`
		Main     string   `json:"main"`
		Versions []listed `json:"versions"`
	}{Repo: repo, Main: idx.Main, Versions: []listed{}}
	for _, v := range idx.Versions {
		files := 0
		m, err := l.readManifest(r.Context(), repo, v.Version)
		if err != nil {
			log.Printf("push listing manifest %s: %v", local.ManifestKey(repo, v.Version), err)
			writeErr(w, http.StatusInternalServerError, "manifest read failed")
			return
		}
		if m != nil {
			files = len(m.Files)
		}
		out.Versions = append(out.Versions, listed{
			Version: v.Version, Commit: v.Commit, SealedAt: v.SealedAt, Files: files,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// muFor returns the per-version staging/seal lock.
func (l *Lane) muFor(vkey string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()
	if mu, ok := l.locks[vkey]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	l.locks[vkey] = mu
	return mu
}

// muForRepo returns the per-repo index read-modify-write lock.
func (l *Lane) muForRepo(repo string) *sync.Mutex {
	l.idxMu.Lock()
	defer l.idxMu.Unlock()
	if mu, ok := l.repoLock[repo]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	l.repoLock[repo] = mu
	return mu
}

// cutRepo splits "org/name/rest" into repo and rest (same shape as proxy's).
func cutRepo(rest string) (repo, tail string) {
	org, after, ok := strings.Cut(rest, "/")
	if !ok {
		return "", ""
	}
	name, tail, _ := strings.Cut(after, "/")
	return org + "/" + name, tail
}

// segRe matches org/name segments (same shape as proxy's).
var segRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func validRepo(repo string) bool {
	org, name, ok := strings.Cut(repo, "/")
	return ok &&
		segRe.MatchString(org) && !strings.Contains(org, "..") &&
		segRe.MatchString(name) && !strings.Contains(name, "..")
}

// validVersion accepts vNNN-style tags or 64-hex digests.
func validVersion(version string) bool {
	if version == "" || len(version) > 128 {
		return false
	}
	return versionRe.MatchString(version) || hex64Re.MatchString(version)
}

// fileRe matches staged file paths (same shape as proxy's validFilePath).
var fileRe = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

func validFilePath(file string) bool {
	if file == "" || len(file) > 511 || !fileRe.MatchString(file) {
		return false
	}
	return !strings.Contains(file, "..") &&
		!strings.Contains(file, "//") &&
		!strings.HasPrefix(file, "/") &&
		!strings.HasSuffix(file, "/")
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
