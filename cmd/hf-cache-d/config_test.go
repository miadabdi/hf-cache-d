package main

import (
	"strings"
	"testing"
)

func TestLoadConfigMissingRequired(t *testing.T) {
	required := []string{"S3_ENDPOINT", "S3_BUCKET", "S3_ACCESS_KEY", "S3_SECRET_KEY"}
	for _, name := range required {
		t.Run("missing_"+name, func(t *testing.T) {
			for _, k := range required {
				t.Setenv(k, "value")
			}
			t.Setenv(name, "")
			_, err := LoadConfig()
			if err == nil {
				t.Fatalf("expected error when %s is empty", name)
			}
			if !strings.Contains(err.Error(), name) {
				t.Errorf("error %q does not mention %s", err, name)
			}
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("S3_ENDPOINT", "http://localhost:8333")
	t.Setenv("S3_BUCKET", "test-bucket")
	t.Setenv("S3_ACCESS_KEY", "test")
	t.Setenv("S3_SECRET_KEY", "test12345678")
	t.Setenv("LISTEN_ADDR", "")
	t.Setenv("HF_UPSTREAM", "")
	t.Setenv("PUSH_TOKEN", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":8080")
	}
	if cfg.HFUpstream != "https://huggingface.co" {
		t.Errorf("HFUpstream = %q, want %q", cfg.HFUpstream, "https://huggingface.co")
	}
	if cfg.PushToken != "" {
		t.Errorf("PushToken = %q, want empty", cfg.PushToken)
	}
}
