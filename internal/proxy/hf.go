// Package proxy serves the public Hugging Face metadata lane: repo info and
// tree listings resolved to commit SHAs, fetched anonymously from upstream,
// and cached pinned by SHA in the S3 store.
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

	"github.com/miadabdi/hf-cache-d/internal/store"
)

// storeAPI is the narrow slice of *store.Store the proxy consumes. The store
// stays a concrete type (no interface at the producer); this consumer-side
// declaration lets offline tests inject an in-memory fake while production
// passes *store.Store, which satisfies it implicitly. Tasks 3/4 reuse it.
type storeAPI interface {
	Get(ctx context.Context, key string) (io.ReadCloser, int64, error)
	Put(ctx context.Context, key string, r io.Reader, size int64) error
}

var _ storeAPI = (*store.Store)(nil)

// refTTL is how long a floating-ref → commit mapping is trusted.
const refTTL = 5 * time.Minute

// Proxy serves the public metadata routes for one upstream.
type Proxy struct {
	upstream string
	store    storeAPI
	client   *http.Client
	now      func() time.Time

	mu   sync.Mutex
	refs map[string]refEntry // "repo/rev" -> commit + expiry
	// ponytail: unbounded ref map, LRU/eviction if repo count grows.

}

type refEntry struct {
	sha    string
	expiry time.Time
}

// New builds a Proxy against the given HF upstream base URL and store.
func New(upstream string, st storeAPI) *Proxy {
	return &Proxy{
		upstream: strings.TrimRight(upstream, "/"),
		store:    st,
		client:   &http.Client{Timeout: 60 * time.Second},
		now:      time.Now,
		refs:     map[string]refEntry{},
	}
}

// Register mounts the metadata routes on mux. The /api/models/ subtree is
// claimed wholesale so the later resolve/file route (Task 3) can live under
// the same prefix without pattern conflicts.
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
// suffix is the canonical (sorted) encoding of the ALLOWLISTED params only
// (see allowQuery): pagination cursors and recursive/expand/limit variants
// get distinct keys, while unknown client params are dropped so arbitrary
// input cannot mint permanent S3 objects.
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
	default:
		p.writeErr(w, http.StatusBadRequest, "unknown path under repo")
		return
	}
	if !validRev(rev) {
		p.writeErr(w, http.StatusBadRequest, "invalid revision")
		return
	}

	sha, err := p.resolve(r.Context(), repo, rev)
	if err != nil {
		p.fail(w, err)
		return
	}

	// One canonical query for both the cache key and the upstream fetch:
	// same allowlist, same ordering — a hit can never serve the wrong body.
	q := allowQuery(r.URL.Query())
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
}

// allowQuery returns the canonical encoding (params sorted) of the
// allowlisted subset of q, dropping everything else.
func allowQuery(q url.Values) url.Values {
	out := url.Values{}
	for k, vs := range q {
		if !allowedParams[k] {
			continue
		}
		for _, v := range vs {
			out.Add(k, v)
		}
	}
	return out
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
// A missing, empty or malformed sha is an error.
func (p *Proxy) upstreamSHA(ctx context.Context, repo, rev string) (string, error) {
	body, _, err := p.fetch(ctx, fmt.Sprintf("/api/models/%s/revision/%s", repo, rev), nil)
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
	env, _ := json.Marshal(cachedResp{Link: link, Body: string(body)})
	if err := p.store.Put(r.Context(), key, bytes.NewReader(env), int64(len(env))); err != nil {
		// Serving fresh data still works; only persistence failed.
		log.Printf("store put %s: %v", key, err)
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
	var env cachedResp
	if err := json.NewDecoder(rc).Decode(&env); err != nil || env.Body == "" {
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

// fail maps internal errors onto client-facing statuses: upstream failures
// keep their mapped status, anything else is a 500 without detail.
func (p *Proxy) fail(w http.ResponseWriter, err error) {
	var ue *upstreamError
	if errors.As(err, &ue) {
		p.writeErr(w, ue.status, ue.msg)
		return
	}
	log.Printf("proxy: %v", err)
	p.writeErr(w, http.StatusInternalServerError, "internal error")
}

// upstreamError carries a client-facing status for upstream failures, plus
// the upstream's real HTTP status (0 for connection-level failures) so the
// retry policy can distinguish 5xx (retry) from 4xx (never retry).
type upstreamError struct {
	status   int // client-facing status
	upstream int // real upstream status, 0 = connection failure
	msg      string
}

func (e *upstreamError) Error() string { return e.msg }

// fetch performs an anonymous GET against upstream, retrying once on
// connection errors and 5xx only. 404 and other 4xx (e.g. rate limits) map
// straight through without retry. It returns the body and, when present, the
// upstream Link header.
func (p *Proxy) fetch(ctx context.Context, path string, q url.Values) (body []byte, link string, err error) {
	// ponytail: 2 attempts total, no backoff — retry policy per plan.
	for attempt := 0; ; attempt++ {
		body, link, err = p.fetchOnce(ctx, path, q)
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

func (p *Proxy) fetchOnce(ctx context.Context, path string, q url.Values) ([]byte, string, error) {
	u := p.upstream + path
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	// The public lane is anonymous: the inbound request's Authorization and
	// Cookie headers never reach upstream.
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", &upstreamError{status: http.StatusBadGateway, msg: "upstream unreachable"}
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, "", &upstreamError{status: http.StatusNotFound, upstream: resp.StatusCode, msg: "not found upstream"}
	case resp.StatusCode >= 400:
		return nil, "", &upstreamError{status: http.StatusBadGateway, upstream: resp.StatusCode, msg: "upstream error"}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", &upstreamError{status: http.StatusBadGateway, msg: "upstream read error"}
	}
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
