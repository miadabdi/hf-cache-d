package main

import (
	"fmt"
	"os"
	"strings"
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
}

// LoadConfig reads the environment and validates required variables,
// failing fast with a message naming the missing variable.
func LoadConfig() (Config, error) {
	cfg := Config{
		ListenAddr:  os.Getenv("LISTEN_ADDR"),
		S3Endpoint:  strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
		S3Bucket:    strings.TrimSpace(os.Getenv("S3_BUCKET")),
		S3AccessKey: strings.TrimSpace(os.Getenv("S3_ACCESS_KEY")),
		S3SecretKey: strings.TrimSpace(os.Getenv("S3_SECRET_KEY")),
		HFUpstream:  strings.TrimSpace(os.Getenv("HF_UPSTREAM")),
		PushToken:   os.Getenv("PUSH_TOKEN"),
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.HFUpstream == "" {
		cfg.HFUpstream = "https://huggingface.co"
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
