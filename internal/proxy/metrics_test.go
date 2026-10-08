package proxy

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/miadabdi/hf-cache-d/internal/metrics"
)

// The metric-accounting contract, per outcome:
//   - metadata MISS: exactly one miss, bytes_pulled +body size (once)
//   - metadata HIT: exactly one hit, no extra bytes_pulled
//   - file warm GET/HEAD: exactly one hit
//   - vanished object self-heal: NO hit, one miss
//   - cold file GET: exactly one miss, bytes_pulled +body size (once)
//   - upstream error: one upstream error, no miss/hit inflation

// newCountedProxy wires a proxy with a private counter set.
func newCountedProxy(t *testing.T, upstream string, st *memStore) (*Proxy, *metrics.Counters) {
	t.Helper()
	p, _ := newTestProxy(t, upstream, st)
	c := &metrics.Counters{}
	p.SetCounters(c)
	return p, c
}

func countOf(t *testing.T, c *metrics.Counters, name string) string {
	t.Helper()
	for _, line := range strings.Split(c.Render(), "\n") {
		if strings.HasPrefix(line, name+" ") {
			return strings.TrimPrefix(line, name+" ")
		}
	}
	t.Fatalf("counter %s missing from render:\n%s", name, c.Render())
	return ""
}

// TestMetricsMetadataBytesPulledOnce: a cold metadata miss must add exactly
// the upstream body size to bytes_pulled (the fetch and the serve must not
// both count it), and a warm hit must add nothing more.
func TestMetricsMetadataBytesPulledOnce(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, c := newCountedProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("cold X-Cache = %q, want MISS", resp.Header.Get("X-Cache"))
	}
	// Two upstream fetches happened (branch resolve + pinned info), each
	// serving the same JSON shape; each must be counted ONCE. The bug this
	// guards against counts the pinned fetch twice (fetch + serve).
	resolveLen := len(`{"sha":"` + sha1 + `","siblings":[{"rfilename":"f-` + sha1[:8] + `"}]}`)
	want := "0"
	if n := 2 * resolveLen; n > 0 {
		want = fmt.Sprint(n)
	}
	if got := countOf(t, c, "hf_cache_bytes_pulled_total"); got != want {
		t.Errorf("bytes_pulled after cold metadata = %s, want %s (resolve+info, each once)", got, want)
	}
	if miss := countOf(t, c, "hf_cache_misses_total"); miss != "1" {
		t.Errorf("misses after cold metadata = %s, want 1", miss)
	}

	// Warm: hit, nothing new pulled.
	proxyGet(t, srv.URL+"/api/models/org/name", nil)
	if got2 := countOf(t, c, "hf_cache_bytes_pulled_total"); got2 != want {
		t.Errorf("bytes_pulled after warm = %s, want unchanged %s", got2, want)
	}
	if hit := countOf(t, c, "hf_cache_hits_total"); hit != "1" {
		t.Errorf("hits after warm = %s, want 1", hit)
	}
}

// TestMetricsFileColdThenWarm: cold file GET = one miss + bytes_pulled equal
// to the body; warm = one hit, nothing new pulled.
func TestMetricsFileColdThenWarm(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("metric file body")
	addFile(t, f, "m.bin", body)
	st := newMemStore()
	p, c := newCountedProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, got := proxyGet(t, srv.URL+"/org/name/resolve/main/m.bin", nil)
	if resp.Header.Get("X-Cache") != "MISS" || string(got) != string(body) {
		t.Fatalf("cold GET X-Cache=%q body=%q", resp.Header.Get("X-Cache"), got)
	}
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return st.has(fileKey(sha1, "org/name", "m.bin")) && files["m.bin"] != ""
	}, "object stored")
	if miss := countOf(t, c, "hf_cache_misses_total"); miss != "1" {
		t.Errorf("misses after cold GET = %s, want 1", miss)
	}
	// resolve JSON (90 bytes, fetched once) + file body (16 bytes, once).
	if pulled := countOf(t, c, "hf_cache_bytes_pulled_total"); pulled != "106" {
		t.Errorf("bytes_pulled after cold GET = %s, want 106 (resolve+body, each once)", pulled)
	}

	resp2, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/m.bin", nil)
	if resp2.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("warm X-Cache = %q, want HIT", resp2.Header.Get("X-Cache"))
	}
	if hit := countOf(t, c, "hf_cache_hits_total"); hit != "1" {
		t.Errorf("hits after warm GET = %s, want 1", hit)
	}
	if pulled := countOf(t, c, "hf_cache_bytes_pulled_total"); pulled != "106" {
		t.Errorf("bytes_pulled after warm = %s, want 106 (unchanged)", pulled)
	}
}

// TestMetricsVanishedObjectNoFalseHit: after the object vanishes under a
// live manifest entry, the self-heal path must count a MISS, never a hit.
func TestMetricsVanishedObjectNoFalseHit(t *testing.T) {
	up, f := newFakeUpstream(t)
	body := []byte("vanish me")
	addFile(t, f, "vf.bin", body)
	st := newMemStore()
	p, c := newCountedProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	proxyGet(t, srv.URL+"/org/name/resolve/main/vf.bin", nil)
	waitFor(t, 5*time.Second, func() bool {
		files := readFileManifest(t, st, sha1)
		return files != nil && files["vf.bin"] != ""
	}, "seed manifest")

	st.delete(fileKey(sha1, "org/name", "vf.bin"))

	before := countOf(t, c, "hf_cache_hits_total")
	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/vf.bin", nil)
	if resp.Header.Get("X-Cache") != "MISS" {
		t.Fatalf("vanished-object GET X-Cache = %q, want MISS", resp.Header.Get("X-Cache"))
	}
	if after := countOf(t, c, "hf_cache_hits_total"); after != before {
		t.Errorf("hits incremented on vanished-object self-heal: %s -> %s (no hit may be counted before the object is confirmed)", before, after)
	}
	if miss := countOf(t, c, "hf_cache_misses_total"); miss != "2" {
		t.Errorf("misses after vanished-object GET = %s, want 2 (cold + self-heal)", miss)
	}
}

// TestMetricsColdRange416OneMiss: a cold ranged GET relaying upstream's
// 416 counts exactly one miss (not one in serveGetMiss and one in relay).
func TestMetricsColdRange416OneMiss(t *testing.T) {
	up, f := newFakeUpstream(t)
	// A cold ranged GET gets upstream 416 through relayColdRange.
	addFile(t, f, "r16.bin", []byte("range sixteen bytes"))
	st := newMemStore()
	p, c := newCountedProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/r16.bin", map[string]string{"Range": "bytes=99-120"})
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("cold ranged GET status = %d, want 416 relayed", resp.StatusCode)
	}
	if miss := countOf(t, c, "hf_cache_misses_total"); miss != "1" {
		t.Errorf("misses after cold ranged 416 = %s, want 1 (counted once)", miss)
	}
}

// TestMetricsNotFoundSeparateFromUpstreamErrors: a legitimate upstream 404
// must count in upstream_not_found_total, NOT in upstream_errors_total —
// routine not-founds must not bury real failures for alerting.
func TestMetricsNotFoundSeparateFromUpstreamErrors(t *testing.T) {
	up, _ := newFakeUpstream(t)
	st := newMemStore()
	p, c := newCountedProxy(t, up.URL, st)
	srv := newTestServer(p)
	defer srv.Close()

	// File that does not exist upstream → 404.
	resp, _ := proxyGet(t, srv.URL+"/org/name/resolve/main/absent.bin", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("absent file status = %d, want 404", resp.StatusCode)
	}
	if nf := countOf(t, c, "hf_cache_upstream_not_found_total"); nf != "1" {
		t.Errorf("upstream_not_found after 404 = %s, want 1", nf)
	}
	if errs := countOf(t, c, "hf_cache_upstream_errors_total"); errs != "0" {
		t.Errorf("upstream_errors after 404 = %s, want 0 (404 is not a failure)", errs)
	}

	// Metadata 404 same discipline.
	proxyGet(t, srv.URL+"/api/models/org/absent-repo", nil)
	if nf := countOf(t, c, "hf_cache_upstream_not_found_total"); nf != "2" {
		t.Errorf("upstream_not_found after metadata 404 = %s, want 2", nf)
	}
	if errs := countOf(t, c, "hf_cache_upstream_errors_total"); errs != "0" {
		t.Errorf("upstream_errors after metadata 404 = %s, want 0", errs)
	}
}
