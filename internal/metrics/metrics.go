// Package metrics holds hf-cache-d's process-wide Prometheus counters,
// served as hand-rolled text exposition at /metricsz. Counters only: no
// client_golang dependency for six numbers.
//
// Semantics:
//   - requests_total{route}: every served request, classified by route
//     (metadata, file, push, healthz, metricsz, index, other).
//   - hits_total / misses_total: requests answered from the cache vs filled
//     from upstream, counted where the X-Cache header is set. Local (sealed)
//     models always count as hits.
//   - upstream_errors_total: upstream fetches that failed after retries
//     (metadata and file lanes).
//   - bytes_served_total: response body bytes written to clients (all
//     routes, counted by the logging middleware).
//   - bytes_pulled_total: body bytes ingested from upstream (metadata and
//     full-file cold pulls) plus bytes staged through the push lane. Ranged
//     cold relays are upstream traffic but are not stored, so they do not
//     count.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Default is the process-wide counter set. It is never replaced, only
// incremented through its atomic fields, so concurrent access is safe.
var Default = &Counters{}

// Counters is the exported counter set.
type Counters struct {
	mu       sync.Mutex
	requests map[string]*atomic.Uint64 // route label -> count

	hits         atomic.Uint64
	misses       atomic.Uint64
	upstreamErrs atomic.Uint64
	upstream404s atomic.Uint64
	bytesServed  atomic.Int64
	bytesPulled  atomic.Int64
}

// AddRequest counts one served request under the route label.
func (c *Counters) AddRequest(route string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.requests == nil {
		c.requests = map[string]*atomic.Uint64{}
	}
	p := c.requests[route]
	if p == nil {
		p = &atomic.Uint64{}
		c.requests[route] = p
	}
	p.Add(1)
}

// AddHit counts one cache-hit response.
func (c *Counters) AddHit() { c.hits.Add(1) }

// AddMiss counts one cache-miss response.
func (c *Counters) AddMiss() { c.misses.Add(1) }

// AddUpstreamError counts one failed (post-retry) upstream fetch.
func (c *Counters) AddUpstreamError() { c.upstreamErrs.Add(1) }

// AddUpstreamNotFound counts one upstream 404: a legitimate not-found
// (unknown repo/file), kept out of upstream_errors_total so routine
// misses cannot bury real failures for alerting.
func (c *Counters) AddUpstreamNotFound() { c.upstream404s.Add(1) }

// AddBytesServed counts response body bytes written to clients.
func (c *Counters) AddBytesServed(n int64) {
	if n > 0 {
		c.bytesServed.Add(n)
	}
}

// AddBytesPulled counts body bytes ingested from upstream or the push lane.
func (c *Counters) AddBytesPulled(n int64) {
	if n > 0 {
		c.bytesPulled.Add(n)
	}
}

// Render emits the counters in Prometheus text exposition format.
func (c *Counters) Render() string {
	c.mu.Lock()
	routes := make([]string, 0, len(c.requests))
	for name, p := range c.requests {
		routes = append(routes, fmt.Sprintf("hf_cache_requests_total{route=%q} %d\n", name, p.Load()))
	}
	c.mu.Unlock()
	sort.Strings(routes)

	var b strings.Builder
	counter := func(name, help string, v uint64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	fmt.Fprintf(&b, "# HELP hf_cache_requests_total Total HTTP requests served, by route.\n# TYPE hf_cache_requests_total counter\n")
	for _, line := range routes {
		b.WriteString(line)
	}
	counter("hf_cache_hits_total", "Requests served from the cache.", c.hits.Load())
	counter("hf_cache_misses_total", "Requests filled from upstream.", c.misses.Load())
	counter("hf_cache_upstream_errors_total", "Upstream fetches that failed after retries.", c.upstreamErrs.Load())
	counter("hf_cache_upstream_not_found_total", "Upstream fetches that legitimately returned 404 (kept out of upstream_errors_total).", c.upstream404s.Load())
	counter("hf_cache_bytes_served_total", "Response body bytes written to clients.", uint64(c.bytesServed.Load()))
	counter("hf_cache_bytes_pulled_total", "Body bytes ingested from upstream or staged via push.", uint64(c.bytesPulled.Load()))
	return b.String()
}
