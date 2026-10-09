package metrics

import (
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// Every non-comment line of the exposition must be a valid Prometheus
// counter sample or HELP/TYPE directive.
var (
	helpRe  = regexp.MustCompile(`^# (HELP|TYPE) hf_cache_[a-z_]+ .+$`)
	lineRe  = regexp.MustCompile(`^hf_cache_(requests_total\{route="[a-z]+"\}|hits_total|misses_total|upstream_errors_total|upstream_not_found_total|bytes_served_total|bytes_pulled_upstream_total|bytes_staged_push_total|first_byte_seconds_total\{route="[a-z]+"\}|first_byte_total\{route="[a-z]+"\}|first_byte_ms_total\{route="[a-z]+"\}) ([0-9]+|\.[0-9]+|[0-9]+\.[0-9]+)$`)
	namesRe = regexp.MustCompile(`(?m)^# TYPE (hf_cache_[a-z_]+) counter$`)
)

func TestRenderIsPrometheusText(t *testing.T) {
	c := &Counters{}
	c.AddRequest("metadata")
	c.AddRequest("file")
	c.AddRequest("file")
	c.AddHit()
	c.AddMiss()
	c.AddMiss()
	c.AddUpstreamError()
	c.AddUpstreamNotFound()
	c.AddUpstreamNotFound()
	c.AddBytesServed(2048)
	c.AddBytesPulledUpstream(1048576)
	c.AddFirstByte("metadata", 5*time.Millisecond)
	out := c.Render()

	for i, line := range strings.Split(strings.TrimRight(out, "\n"), "\n") {
		if !helpRe.MatchString(line) && !lineRe.MatchString(line) {
			t.Errorf("line %d %q is not valid Prometheus text exposition", i+1, line)
		}
	}
	for _, want := range []string{
		`hf_cache_requests_total{route="file"} 2`,
		`hf_cache_requests_total{route="metadata"} 1`,
		"hf_cache_hits_total 1",
		"hf_cache_misses_total 2",
		"hf_cache_upstream_errors_total 1",
		"hf_cache_upstream_not_found_total 2",
		"hf_cache_bytes_served_total 2048",
		"hf_cache_bytes_pulled_upstream_total 1048576",
		"hf_cache_bytes_staged_push_total 0",
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("output missing %q", want)
		}
	}
}

func TestEveryTypedMetricHasCounterType(t *testing.T) {
	out := (&Counters{}).Render()
	typed := map[string]bool{}
	for _, m := range namesRe.FindAllStringSubmatch(out, -1) {
		typed[m[1]] = true
	}
	samples := map[string]bool{}
	for _, m := range regexp.MustCompile(`^(hf_cache_[a-z_]+)[ {]`).FindAllStringSubmatch(out, -1) {
		samples[m[1]] = true
	}
	if len(typed) != 11 {
		t.Errorf("typed metrics = %v, want 11", typed)
	}
	for name := range samples {
		if !typed[name] {
			t.Errorf("sample %s has no # TYPE directive", name)
		}
	}
}

func TestCountersRaceSafe(t *testing.T) {
	c := &Counters{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c.AddRequest("file")
				c.AddHit()
				c.AddBytesServed(10)
			}
		}()
	}
	wg.Wait()
	if !strings.Contains(c.Render(), `hf_cache_requests_total{route="file"} 800`) {
		t.Error("concurrent increments lost")
	}
	if !strings.Contains(c.Render(), "hf_cache_hits_total 800") {
		t.Error("concurrent hits lost")
	}
	if !strings.Contains(c.Render(), "hf_cache_bytes_served_total 8000") {
		t.Error("concurrent bytes lost")
	}
}

func TestNegativeBytesIgnored(t *testing.T) {
	c := &Counters{}
	c.AddBytesServed(-5)
	c.AddBytesPulledUpstream(-5)
	c.AddBytesStagedPush(-5)
	out := c.Render()
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "hf_cache_") && strings.Contains(line, " -") {
			t.Errorf("negative sample value leaked: %q", line)
		}
	}
}

// TestMetricSplitsAndLatency: bytes pulled from upstream and bytes staged
// via push are SEPARATE counters (the fleet could not tell re-pull
// overhead from push traffic), and first-byte latency exists per route as
// sum+count (average = sum/count) — the pair that would have exposed the
// byteless-waiter defect instantly.
func TestMetricSplitsAndLatency(t *testing.T) {
	c := &Counters{}
	c.AddBytesPulledUpstream(1000)
	c.AddBytesStagedPush(2000)
	c.AddFirstByte("file", 150*time.Millisecond)
	c.AddFirstByte("file", 250*time.Millisecond)

	out := c.Render()
	for _, want := range []string{
		"hf_cache_bytes_pulled_upstream_total 1000",
		"hf_cache_bytes_staged_push_total 2000",
		`hf_cache_first_byte_seconds_total{route="file"} 0.4`,
		`hf_cache_first_byte_total{route="file"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q:\n%s", want, out)
		}
	}
}
