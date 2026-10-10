package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full runtime configuration, sourced from environment variables.
type Config struct {
	ListenAddr  string // LISTEN_ADDR, default ":8080"
	S3Endpoint  string // S3_ENDPOINT, required
	S3Bucket    string // S3_BUCKET, required
	S3AccessKey string // S3_ACCESS_KEY, required
	S3SecretKey string // S3_SECRET_KEY, required
	HFUpstream  string // HF_UPSTREAM, default "https://huggingface.co"
	PushToken   string // PUSH_TOKEN, empty means push lane disabled

	// UpstreamToken authenticates UPSTREAM fetches (HF_UPSTREAM_TOKEN) so the
	// mirror can cache gated repos. Empty = fully anonymous (v0.1 behavior).
	// Inbound client tokens are always stripped — the token never leaks to
	// readers, readers never need one.
	UpstreamToken string // HF_UPSTREAM_TOKEN, empty means anonymous upstream

	// IntegrityCheckInterval is how often the background self-check verifies
	// one random manifest's files (sha256 of stored bytes vs the manifest).
	// Zero disables the self-check entirely.
	IntegrityCheckInterval time.Duration // INTEGRITY_CHECK_INTERVAL, default 1h

	// MaxColdTransfers caps concurrent cold upstream transfers (leaders +
	// background warms). Overflow requests stream through uncached instead
	// of queueing — every client still gets bytes; the cap protects the
	// shared egress (VPN) and the object store from transfer storms.
	MaxColdTransfers int // MAX_COLD_TRANSFERS, default 3
}

// LoadConfig reads the environment and validates required variables,
// failing fast with a message naming the missing variable.
func LoadConfig() (Config, error) {
	cfg := Config{
		ListenAddr:    os.Getenv("LISTEN_ADDR"),
		S3Endpoint:    strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
		S3Bucket:      strings.TrimSpace(os.Getenv("S3_BUCKET")),
		S3AccessKey:   strings.TrimSpace(os.Getenv("S3_ACCESS_KEY")),
		S3SecretKey:   strings.TrimSpace(os.Getenv("S3_SECRET_KEY")),
		HFUpstream:    strings.TrimSpace(os.Getenv("HF_UPSTREAM")),
		PushToken:     os.Getenv("PUSH_TOKEN"),
		UpstreamToken: strings.TrimSpace(os.Getenv("HF_UPSTREAM_TOKEN")),
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.HFUpstream == "" {
		cfg.HFUpstream = "https://huggingface.co"
	}
	cfg.IntegrityCheckInterval = time.Hour
	if v := strings.TrimSpace(os.Getenv("INTEGRITY_CHECK_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return Config{}, fmt.Errorf("INTEGRITY_CHECK_INTERVAL must be a non-negative duration (e.g. 1h, 30m), got %q", v)
		}
		cfg.IntegrityCheckInterval = d // 0 disables
	}

	cfg.MaxColdTransfers = 3
	if v := strings.TrimSpace(os.Getenv("MAX_COLD_TRANSFERS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("MAX_COLD_TRANSFERS must be a positive integer, got %q", v)
		}
		cfg.MaxColdTransfers = n
	}

	switch {
	case cfg.S3Endpoint == "":
		return Config{}, fmt.Errorf("S3_ENDPOINT is required")
	case cfg.S3Bucket == "":
		return Config{}, fmt.Errorf("S3_BUCKET is required")
	case cfg.S3AccessKey == "":
		return Config{}, fmt.Errorf("S3_ACCESS_KEY is required")
	case cfg.S3SecretKey == "":
		return Config{}, fmt.Errorf("S3_SECRET_KEY is required")
	}
	return cfg, nil
}
