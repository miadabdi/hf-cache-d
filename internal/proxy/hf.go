// Package proxy serves the public Hugging Face lanes: repo info and tree
// listings (metadata lane) plus file resolve with tee-through caching (file
// lane), fetched anonymously from upstream and cached pinned by commit SHA
// in the S3 store.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/local"
	"github.com/miadabdi/hf-cache-d/internal/manifest"
	"github.com/miadabdi/hf-cache-d/internal/metrics"
	"github.com/miadabdi/hf-cache-d/internal/store"
	"golang.org/x/sync/semaphore"
)

// storeAPI is the narrow slice of *store.Store the proxy consumes. The store
// stays a concrete type (no interface at the producer); this consumer-side
// declaration lets offline tests inject an in-memory fake while production
// passes *store.Store, which satisfies it implicitly. Tasks 3/4 reuse it.
type storeAPI interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Head(ctx context.Context, key string) (bool, int64, error)
}

var _ storeAPI = (*store.Store)(nil)

// refTTL is how long a floating-ref → commit mapping is trusted.
const refTTL = 5 * time.Minute

// metadataMaxBytes bounds both fetched JSON and decoded cache envelopes.
const metadataMaxBytes = 32 << 20 // 32 MiB

// Proxy serves the public metadata and file routes for one upstream.
type Proxy struct {
	upstream string
	store    storeAPI
	client   *http.Client // metadata lane: small bodies, 60s overall deadline
	// fileClient carries file transfers. No overall deadline: a cold pull
	// detached from its client request must run to completion however long
	// the body takes. It is bounded only by the context of the transfer
	// (server-lifetime for detached cold pulls).
	// ponytail: unbounded transfer time is the trusted-network ceiling; add
	// a deadline/semaphore if exposed to untrusted networks.
	fileClient *http.Client
	// headClient HEADs the resolve URL WITHOUT following the CDN redirect,
	// so headers come from HF's own hop (X-Linked-ETag sha256), not the
	// CDN's CAS ETag. See New for the why.
	headClient *http.Client
	now        func() time.Time
	// upstreamToken, when set (HF_UPSTREAM_TOKEN), rides every outbound
	// upstream request so gated repos can be fetched and cached. Inbound
	// client Authorization is always stripped (see authUpstream callers).
	upstreamToken string

	mu   sync.Mutex
	refs map[string]refEntry // "repo/rev" -> commit + expiry
	// ponytail: unbounded ref map, LRU/eviction if repo count grows.

	// File-lane state (Task 3).
	manMu     sync.Mutex
	manifests map[string]*manifest.Manifest // repo-scoped or seal key -> read cache
	relMu     sync.Mutex
	releaseMu map[string]*sync.Mutex // repo-scoped manifest key -> RMW lock

	// Singleflight + fan-out state: one upstream transfer per object key.
	sfMu      sync.Mutex
	inflightT map[string]*transfer // object key -> live transfer

	// coldSlots caps concurrent cold transfers (MAX_COLD_TRANSFERS, default
	// 3; nil = uncapped, tests). Overflow requests stream through uncached.
	coldSlots *semaphore.Weighted

	// Sealed local models (Task 4). nil until SetLocalIndexes wires the
	// push lane's index cache; reads then check locals BEFORE the public
	// flow (the shadowing rule).
	locals *local.Indexes

	// m is the process-wide counter set the lanes increment. It is the
	// metrics.Default singleton, never nil in production; the field exists
	// so tests can assert on a private instance.
	m *metrics.Counters
}

type refEntry struct {
	sha    string
	expiry time.Time
}

// New builds a Proxy against the given HF upstream base URL and store.
func New(upstream string, st storeAPI) *Proxy {
	// Per-lane transports: saturating file transfers must not starve small
	// metadata API calls of connections (fleet issue #3 — revision GETs
	// 502'd for minutes during multi-GB pulls). Nil Transport would fall
	// back to the SHARED http.DefaultTransport, defeating the isolation.
	metaTransport := &http.Transport{
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
	}
	fileTransport := &http.Transport{
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     90 * time.Second,
	}
	return &Proxy{
		upstream:   strings.TrimRight(upstream, "/"),
		store:      st,
		client:     &http.Client{Timeout: 60 * time.Second, Transport: metaTransport},
		fileClient: &http.Client{Transport: fileTransport},
		// headClient stops at HF's 302 resolve hop: the pre-redirect response
		// carries X-Linked-ETag/X-Linked-Size (the sha256 truth), while the
		// CDN hop's own ETag is a CAS-style hash for xet-backed files that
		// would flip against our warm sha256 ETags.
		headClient: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: 60 * time.Second,
		},
		now:       time.Now,
		refs:      map[string]refEntry{},
		manifests: map[string]*manifest.Manifest{},
		releaseMu: map[string]*sync.Mutex{},
		m:         metrics.Default,
	}
}

// SetMaxColdTransfers installs the concurrent-cold-transfer cap (tests
// default to nil = uncapped; main wires the configured value).
func (p *Proxy) SetMaxColdTransfers(n int) {
	if n > 0 {
		p.coldSlots = semaphore.NewWeighted(int64(n))
	}
}

// SetCounters installs a private counter set (tests).
func (p *Proxy) SetCounters(c *metrics.Counters) { p.m = c }

// SetUpstreamToken authenticates UPSTREAM requests (HF_UPSTREAM_TOKEN) so
// the mirror can fetch gated repos. Inbound client credentials are always
// stripped — this token never reaches a reader, and readers stay anonymous.
func (p *Proxy) SetUpstreamToken(tok string) { p.upstreamToken = tok }

// authUpstream sets the configured upstream bearer token, if any. Called on
// every outbound request the mirror itself constructs.
func (p *Proxy) authUpstream(req *http.Request) {
	if p.upstreamToken != "" {
		req.Header.Set("Authorization", "Bearer "+p.upstreamToken)
	}
}

// Register mounts the metadata routes on mux. The /api/models/ subtree is
// claimed wholesale; the file lane (RegisterFiles) cannot share this mux —
// see RegisterFiles for the Go 1.22 precedence constraint — so newMux mounts
// it on a child mux under "/".
func (p *Proxy) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/models/", p.handleModels)
}

// handleModels routes everything under /api/models/. The path is parsed
// manually because repo IDs are exactly {org}/{name} (two segments) and
// malformed shapes must yield a 400 JSON error rather than a route miss.
//
// S3 key layout (documented contract, reused by Tasks 3/4):
//
//	pub/<sha[0:2]>/<sha>/api/models/<org>/<name>/info.json[?<canonical query>]
//	pub/<sha[0:2]>/<sha>/api/models/<org>/<name>/tree.json[?<canonical query>]
//
// The API response is stored as a "file" under the repo path. The query
// suffix is the canonical (sorted) encoding of ALLOWLISTED params only.
// Duplicate values keep the first, values above 1024 bytes and malformed
// cursors get 400, and unknown params are dropped. Legitimate distinct
// cursors still mint permanent keys (no-GC ceiling).
func (p *Proxy) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		p.writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api/models/")

	repo, tail := cutRepo(rest)
	if !validRepo(repo) {
		p.writeErr(w, http.StatusBadRequest, "invalid repo id: want {org}/{name}")
		return
	}

	var kind, rev string
	switch {
	case tail == "":
		kind, rev = "info", "main" // default branch
	case strings.HasPrefix(tail, "revision/"):
		kind, rev = "info", strings.TrimPrefix(tail, "revision/")
	case strings.HasPrefix(tail, "tree/"):
		kind, rev = "tree", strings.TrimPrefix(tail, "tree/")
	case strings.HasPrefix(tail, "xet-read-token/"):
		// hub 1.x probes this on xet-backed repos before classic resolve.
		// The mirror serves classic resolve only; a plain 404 is exactly
		// what a non-xet repo answers, so the client falls back cleanly
		// (no HF_HUB_DISABLE_XET needed, no opaque BadRequestError).
		p.writeErr(w, http.StatusNotFound, "xet not supported; use classic resolve")
		return
	default:
		p.writeErr(w, http.StatusBadRequest, "unknown path under repo")
		return
	}
	if !validRev(rev) {
		p.writeErr(w, http.StatusBadRequest, "invalid revision")
		return
	}

	// Sealed local models shadow the public repo for every revision: check
	// FIRST, and never fall back to the public flow once sealed.
	if p.handleModelsLocal(w, r, repo, kind, rev) {
		return
	}

	sha, err := p.resolve(r.Context(), repo, rev)
	if err != nil {
		p.fail(w, err)
		return
	}

	// One canonical query for both the cache key and the upstream fetch:
	// same allowlist, same ordering — a hit can never serve the wrong body.
	q, err := allowQuery(r.URL.Query())
	if err != nil {
		p.writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	key := fmt.Sprintf("pub/%s/%s/api/models/%s/%s.json", sha[:2], sha, repo, kind)
	if enc := q.Encode(); enc != "" {
		key += "?" + enc
	}
	p.servePinned(w, r, repo, sha, key, kind, q)
}

// allowedParams are the tree params the HF API actually honors. Anything
// else a client sends is dropped from both the cache key and the upstream
// fetch (HF ignores unknown params), so arbitrary input cannot mint
// unbounded permanent S3 keys.
var allowedParams = map[string]bool{
	"recursive": true,
	"cursor":    true,
	"expand":    true,
	"limit":     true,
	"blobs":     true, // sibling size info — the param hf_hub 0.36.x actually sends on the wire
}

// allowQuery canonicalizes allowlisted values. Duplicate occurrences keep
// only the first (so they cannot mint keys); malformed/oversized values get
// 400. Unknown parameters are dropped.
var cursorRe = regexp.MustCompile(`^[A-Za-z0-9._~=/+\-]*$`)

func allowQuery(q url.Values) (url.Values, error) {
	out := url.Values{}
	for k, vs := range q {
		if !allowedParams[k] || len(vs) == 0 {
			continue
		}
		v := vs[0]
		if len(v) > 1024 || (k == "cursor" && !cursorRe.MatchString(v)) {
			return nil, fmt.Errorf("invalid %s query value", k)
		}
		out.Set(k, v)
	}
	return out, nil
}

// cutRepo splits the path remainder after /api/models/ into the {org}/{name}
// repo id and whatever follows it.
func cutRepo(rest string) (repo, tail string) {
	org, after, ok := strings.Cut(rest, "/")
	if !ok {
		return "", "" // single segment: no repo
	}
	name, tail, _ := strings.Cut(after, "/")
	return org + "/" + name, tail
}

// segRe is the per-segment shape of org and name (HF-like): alphanumerics
// plus . _ -, must start alphanumeric, no "..", length-capped.
var segRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func validRepo(repo string) bool {
	org, name, ok := strings.Cut(repo, "/")
	return ok &&
		segRe.MatchString(org) && !strings.Contains(org, "..") &&
		segRe.MatchString(name) && !strings.Contains(name, "..")
}

var (
	shaRe  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	revSeg = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// validRev accepts a 40-hex commit SHA or a branch/tag name. It rejects
// anything with path-traversal or empty segments.
func validRev(rev string) bool {
	if shaRe.MatchString(rev) {
		return true
	}
	return revSeg.MatchString(rev) && !strings.Contains(rev, "..")
}

// resolve maps rev to a commit SHA. A 40-hex rev is used directly; anything
// else is resolved via upstream and memoized for refTTL. Resolution failures
// are never memoized.
func (p *Proxy) resolve(ctx context.Context, repo, rev string) (string, error) {
	if shaRe.MatchString(rev) {
		return rev, nil
	}
	cacheKey := repo + "/" + rev
	p.mu.Lock()
	if e, ok := p.refs[cacheKey]; ok && p.now().Before(e.expiry) {
		p.mu.Unlock()
		return e.sha, nil
	}
	p.mu.Unlock()

	sha, err := p.upstreamSHA(ctx, repo, rev)
	if err != nil {
		return "", err
	}
	p.mu.Lock()
	p.refs[cacheKey] = refEntry{sha: sha, expiry: p.now().Add(refTTL)}
	p.mu.Unlock()
	return sha, nil
}

// upstreamSHA fetches /api/models/{repo}/revision/{rev} and extracts sha.
// A missing, empty or malformed sha is an error. Typed 404 scoping: a
// failing DEFAULT-branch resolve means the repo is gone (RepoNotFound); a
// failing explicit revision means the revision is gone (RevisionNotFound) —
// the repo usually exists when a client got its id from a listing.
func (p *Proxy) upstreamSHA(ctx context.Context, repo, rev string) (string, error) {
	code := "RevisionNotFound"
	if rev == "main" {
		code = "RepoNotFound"
	}
	body, _, err := p.fetchCode(ctx, fmt.Sprintf("/api/models/%s/revision/%s", repo, rev), nil, code)
	if err != nil {
		return "", err
	}
	var info struct {
		SHA string `json:"sha"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		// Non-JSON 2xx body from upstream is an upstream fault, not ours.
		return "", &upstreamError{status: http.StatusBadGateway, msg: "upstream revision response is not JSON"}
	}
	if !shaRe.MatchString(info.SHA) {
		// Malformed (including empty) sha is an upstream fault, not a
		// cacheable success and not our bug: surface as 502.
		return "", &upstreamError{status: http.StatusBadGateway, msg: "upstream revision response has malformed sha"}
	}
	return info.SHA, nil
}

// cachedResp is the stored envelope: the exact upstream body plus its Link
// header (tree pagination). The Link must survive caching or a warm page-1
// response would masquerade as the last page and truncate client listings.
type cachedResp struct {
	Link string `json:"link,omitempty"`
	Body string `json:"body"`
}

// servePinned serves kind's body for repo@sha from the store, fetching and
// storing on miss. The resolved commit is exposed via X-Repo-Commit. q is
// the allowlisted canonical query used for both key and upstream fetch.
func (p *Proxy) servePinned(w http.ResponseWriter, r *http.Request, repo, sha, key, kind string, q url.Values) {
	if body, link, ok := p.readCached(r.Context(), key); ok {
		p.writeBody(w, r, body, "HIT", sha, link)
		return
	}

	// Cache miss: fetch the SHA-pinned body from upstream. Even for the
	// default-branch route the fetch is pinned (not the ref), so stored
	// bytes stay immutable per commit.
	path := fmt.Sprintf("/api/models/%s/revision/%s", repo, sha)
	if kind == "tree" {
		path = fmt.Sprintf("/api/models/%s/tree/%s", repo, sha)
	}
	body, link, err := p.fetch(r.Context(), path, q)
	if err != nil {
		p.fail(w, err)
		return
	}
	// bytes_pulled is counted inside fetchOnce (where the body is read);
	// counting it here too would double every metadata miss.
	env, _ := json.Marshal(cachedResp{Link: link, Body: string(body)})
	if len(env) <= metadataMaxBytes {
		if err := p.store.Put(r.Context(), key, bytes.NewReader(env), int64(len(env))); err != nil {
			// Serving fresh data still works; only persistence failed.
			log.Printf("store put %s: %v", key, err)
		}
	}

	p.writeBody(w, r, body, "MISS", sha, link)
}

// readCached loads and unwraps a stored envelope. A decode failure is
// treated as a miss so corrupt objects self-heal on refetch.
func (p *Proxy) readCached(ctx context.Context, key string) (body []byte, link string, ok bool) {
	rc, _, err := p.store.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("store get %s: %v", key, err)
		}
		return nil, "", false
	}
	defer rc.Close()
	// Bound the encoded object as well as its decoded metadata body.
	raw, err := io.ReadAll(io.LimitReader(rc, metadataMaxBytes+1))
	if err != nil || len(raw) > metadataMaxBytes {
		return nil, "", false
	}
	var env cachedResp
	if err := json.Unmarshal(raw, &env); err != nil || env.Body == "" || len(env.Body) > metadataMaxBytes {
		log.Printf("cached %s: not a valid envelope, refetching", key)
		return nil, "", false
	}
	return []byte(env.Body), env.Link, true
}

// writeBody emits a 200 JSON body with cache-status and commit headers, plus
// an optional Link header for tree pagination. The link is rewritten against
// the current request even on cache hits, so a warm page-1 always carries a
// next-page URL pointing at this service.
func (p *Proxy) writeBody(w http.ResponseWriter, r *http.Request, body []byte, cache, sha, link string) {
	switch cache {
	case "HIT":
		p.m.AddHit()
	case "MISS":
		p.m.AddMiss()
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Cache", cache)
	h.Set("X-Repo-Commit", sha)
	h.Set("Content-Length", fmt.Sprint(len(body)))
	if link != "" {
		if nl, ok := rewriteLink(link, r); ok {
			h.Set("Link", nl)
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// writeErr emits the JSON error shape used across the proxy lanes.
func (p *Proxy) writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// writeErrCode is writeErr with an HF-compatible X-Error-Code header: stock
// huggingface_hub raises typed exceptions (RepositoryNotFoundError,
// EntryNotFoundError) from these codes; without them every 404 is untyped.
func (p *Proxy) writeErrCode(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("X-Error-Code", code)
	p.writeErr(w, status, msg)
}

// fail maps internal errors onto client-facing statuses: upstream failures
// keep their mapped status, anything else is a 500 without detail. Legit
// 404s count as not-found, never as errors, so routine misses cannot bury
// real failures in upstream_errors_total.
func (p *Proxy) fail(w http.ResponseWriter, err error) {
	var ue *upstreamError
	if errors.As(err, &ue) {
		if ue.status == http.StatusNotFound {
			p.m.AddUpstreamNotFound()
		} else {
			p.m.AddUpstreamError()
		}
		if ue.code != "" {
			w.Header().Set("X-Error-Code", ue.code)
		}
		p.writeErr(w, ue.status, ue.msg)
		return
	}
	log.Printf("proxy: %v", err)
	p.writeErr(w, http.StatusInternalServerError, "internal error")
}

// upstreamError carries a client-facing status for upstream failures, plus
// the upstream's real HTTP status (0 for connection-level failures) so the
// retry policy can distinguish 5xx (retry) from 4xx (never retry). code is
// the HF-compatible X-Error-Code for 404s (RepoNotFound/EntryNotFound).
type upstreamError struct {
	status   int // client-facing status
	upstream int // real upstream status, 0 = connection failure
	msg      string
	code     string // X-Error-Code, set on 404s
}

func (e *upstreamError) Error() string { return e.msg }

// fetch performs an anonymous GET against upstream, retrying once on
// connection errors and 5xx only. 404 and other 4xx (e.g. rate limits) map
// straight through without retry. It returns the body and, when present, the
// upstream Link header. The 404 X-Error-Code is caller-scoped: fetchCode
// for callers that know which entity the path identifies (repo vs
// revision), RepoNotFound otherwise.
func (p *Proxy) fetch(ctx context.Context, path string, q url.Values) (body []byte, link string, err error) {
	return p.fetchC(ctx, path, q, "RepoNotFound")
}

func (p *Proxy) fetchCode(ctx context.Context, path string, q url.Values, code string) (body []byte, link string, err error) {
	return p.fetchC(ctx, path, q, code)
}

func (p *Proxy) fetchC(ctx context.Context, path string, q url.Values, code string) (body []byte, link string, err error) {
	// ponytail: 2 attempts total, no backoff — retry policy per plan.
	for attempt := 0; ; attempt++ {
		body, link, err = p.fetchOnce(ctx, path, q, code)
		if err == nil {
			return body, link, nil
		}
		if attempt == 1 || !retriable(err) {
			return nil, "", err
		}
	}
}

// retriable reports whether err is a connection failure or an upstream 5xx.
func retriable(err error) bool {
	var ue *upstreamError
	if errors.As(err, &ue) {
		return ue.upstream == 0 || ue.upstream >= 500
	}
	return false // bare errors (e.g. request build) are not retried
}

func (p *Proxy) fetchOnce(ctx context.Context, path string, q url.Values, code string) ([]byte, string, error) {
	u := p.upstream + path
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	// The public lane is anonymous: the inbound request's Authorization and
	// Cookie headers never reach upstream. The MIRROR's own token (if
	// configured) does — that is the gated-repo fetch path.
	p.authUpstream(req)
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", &upstreamError{status: http.StatusBadGateway, msg: "upstream unreachable"}
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", &upstreamError{status: http.StatusNotFound, upstream: resp.StatusCode, msg: "not found upstream", code: code}
	case resp.StatusCode == http.StatusUnauthorized:
		// HF answers 401 for anonymous requests to nonexistent/private
		// repos; to an anonymous mirror that is indistinguishable from
		// unknown-repo, and stock clients expect the typed 404.
		return nil, "", &upstreamError{status: http.StatusNotFound, upstream: resp.StatusCode, msg: "repository not found", code: code}
	case resp.StatusCode >= 400:
		return nil, "", &upstreamError{status: http.StatusBadGateway, upstream: resp.StatusCode, msg: "upstream error"}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, metadataMaxBytes+1))
	if err != nil {
		return nil, "", &upstreamError{status: http.StatusBadGateway, msg: "upstream read error"}
	}
	if len(body) > metadataMaxBytes {
		return nil, "", &upstreamError{status: http.StatusBadGateway, upstream: resp.StatusCode, msg: "upstream metadata too large"}
	}
	p.m.AddBytesPulled(int64(len(body)))
	return body, resp.Header.Get("Link"), nil
}

// rewriteLink points an upstream Link header (rel="next" pagination) at this
// service: scheme and host swapped for the inbound request's, path and query
// preserved from the upstream URL so the cursor travels with it.
func rewriteLink(link string, r *http.Request) (string, bool) {
	start := strings.IndexByte(link, '<')
	end := strings.IndexByte(link, '>')
	if start < 0 || end <= start {
		return "", false
	}
	u, err := url.Parse(link[start+1 : end])
	if err != nil || u.Host == "" {
		return "", false
	}
	u.Scheme = "http"
	if r.TLS != nil {
		u.Scheme = "https"
	}
	u.Host = r.Host
	rest := ""
	if end+1 < len(link) {
		rest = strings.TrimSpace(link[end+1:])
	}
	out := "<" + u.String() + ">"
	if rest != "" {
		out += "; " + rest
	}
	return out, true
}
