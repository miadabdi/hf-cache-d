package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/miadabdi/hf-cache-d/internal/store"
)

// newTestMux wires the mux with a proxy against a dead upstream: handler
// tests here never touch the metadata lane's upstream path.
func newTestMux() http.Handler {
	mux, _ := newMuxAny("http://127.0.0.1:0", "", &nopStore{}, 0)
	return mux
}

type nopStore struct{}

func (nopStore) Get(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return nil, 0, store.ErrNotFound
}
func (nopStore) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	return nil, store.ErrNotFound
}
func (nopStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	return nil
}
func (nopStore) Head(ctx context.Context, key string) (bool, int64, error) {
	return false, 0, nil
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(newTestMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "ok" || got["version"] != "dev" {
		t.Errorf("body = %v, want status=ok version=dev", got)
	}
}

func TestIndexRoutes(t *testing.T) {
	srv := httptest.NewServer(newTestMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	routes := got["routes"]
	if len(routes) < 9 || routes[0] != "/healthz" || routes[len(routes)-1] != "/" {
		t.Errorf("routes = %v, want 9 routes incl. /healthz and /", routes)
	}
	joined := strings.Join(routes, ",")
	for _, want := range []string{"/api/models/{repo}", "/revision/{rev}", "/tree/{rev}"} {
		if !strings.Contains(joined, want) {
			t.Errorf("routes = %v, want %s listed", routes, want)
		}
	}
}

// TestWhoamiV2 pins the honest stub: token-validating tooling (huggingface-cli
// login/whoami, HfApi.whoami) must get a well-formed answer, not a 404; we
// serve anonymously and never pretend to validate tokens.
func TestWhoamiV2(t *testing.T) {
	h, _ := newMuxAny("http://upstream.invalid", "", newMemStore(), 0)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/whoami-v2")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("whoami-v2 status = %d, want 200", resp.StatusCode)
	}
	var out struct {
		Type string `json:"type"`
		Name string `json:"name"`
		Auth struct {
			AccessToken struct {
				Role string `json:"role"`
			} `json:"accessToken"`
		} `json:"auth"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode whoami-v2: %v", err)
	}
	if out.Type != "user" || out.Name != "anonymous" || out.Auth.AccessToken.Role != "read" {
		t.Errorf("whoami-v2 = %+v, want anonymous read user", out)
	}
}
