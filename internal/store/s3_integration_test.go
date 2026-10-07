//go:build s3compose

// Integration tests against a live SeaweedFS S3 gateway started by
// docker-compose.yml + scripts/dev-s3.sh. They never start docker
// themselves and skip (with a clear message) when the endpoint is down.

package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// testEndpoint returns the integration endpoint, S3_TEST_ENDPOINT or the
// compose default.
func testEndpoint() string {
	if e := os.Getenv("S3_TEST_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8333"
}

// fixtureCreds are the throwaway credentials registered by dev-s3.sh.
func fixtureCreds() (accessKey, secretKey, bucket string) {
	accessKey = envOr("S3_ACCESS_KEY", "test")
	secretKey = envOr("S3_SECRET_KEY", "test12345678")
	bucket = envOr("S3_BUCKET", "test-bucket")
	return
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	endpoint := testEndpoint()
	hostport := endpoint
	for _, prefix := range []string{"http://", "https://"} {
		if len(hostport) >= len(prefix) && hostport[:len(prefix)] == prefix {
			hostport = hostport[len(prefix):]
		}
	}
	conn, err := net.DialTimeout("tcp", hostport, 2*time.Second)
	if err != nil {
		t.Skipf("S3 endpoint %s not reachable (start with: docker compose up -d && ./scripts/dev-s3.sh): %v", endpoint, err)
	}
	conn.Close()

	accessKey, secretKey, bucket := fixtureCreds()
	st, err := New(endpoint, bucket, accessKey, secretKey)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Reachability is not enough: another local SeaweedFS may squat on the
	// port with different credentials. A non-NotFound error here means the
	// endpoint is not OUR fixture; skip with instructions instead of
	// producing a confusing failure.
	if _, _, err := st.Get(context.Background(), prefix()+"auth-probe"); err != nil && !errors.Is(err, ErrNotFound) {
		t.Skipf("S3 endpoint %s reachable but not usable with fixture creds (expected when another SeaweedFS owns the port; fix with: docker compose down && SEAWEEDFS_S3_PORT=8333 docker compose up -d && ./scripts/dev-s3.sh): %v", endpoint, err)
	}
	return st
}

// prefix returns a unique key prefix so parallel/serial runs never collide.
func prefix() string { return fmt.Sprintf("t/%d/", time.Now().UnixNano()) }

func TestGetMissingIsErrNotFound(t *testing.T) {
	st := newTestStore(t)
	_, _, err := st.Get(context.Background(), prefix()+"does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
	if rrc, rerr := st.GetRange(context.Background(), prefix()+"does-not-exist", 0, 1); !errors.Is(rerr, ErrNotFound) {
		if rrc != nil {
			rrc.Close()
		}
		t.Fatalf("GetRange missing: err = %v, want ErrNotFound", rerr)
	}
	exists, _, err := st.Head(context.Background(), prefix()+"does-not-exist")
	if err != nil {
		t.Fatalf("Head missing: %v", err)
	}
	if exists {
		t.Fatal("Head missing: exists = true, want false")
	}
}

func TestBackendErrorIsNotErrNotFound(t *testing.T) {
	newTestStore(t) // asserts the endpoint is reachable; skip if not
	// A store pointed at a nonexistent bucket surfaces a backend error,
	// which must NOT be confused with ErrNotFound for an object miss.
	bad, err := New(testEndpoint(), "no-such-bucket-xyz", "test", "test12345678")
	if err != nil {
		t.Fatal(err)
	}
	_, _, gerr := bad.Get(context.Background(), prefix()+"k")
	if gerr == nil {
		t.Fatal("expected error for missing bucket")
	}
	if errors.Is(gerr, ErrNotFound) {
		t.Fatalf("missing-bucket error must not be ErrNotFound, got %v", gerr)
	}
}

func TestPutGetRoundtripMultiMB(t *testing.T) {
	st := newTestStore(t)
	const size = 8<<20 + 137 // 8 MiB + change
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	key := prefix() + "big.bin"

	if err := st.Put(context.Background(), key, bytes.NewReader(data), int64(size)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, gotSize, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if int64(len(got)) != gotSize {
		t.Errorf("Get size = %d, body = %d bytes", gotSize, len(got))
	}
	if int64(len(got)) != size {
		t.Errorf("roundtrip length = %d, want %d", len(got), size)
	}
	gotSum := sha256.Sum256(got)
	if gotSum != sum {
		t.Error("sha256 mismatch after roundtrip")
	}
}

func TestPutUnknownSizeMultipart(t *testing.T) {
	st := newTestStore(t)
	const size = 12 << 20 // > 5 MiB part size, exercises multipart
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	key := prefix() + "multipart.bin"
	if err := st.Put(context.Background(), key, bytes.NewReader(data), -1); err != nil {
		t.Fatalf("Put(unknown size): %v", err)
	}
	rc, _, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("multipart roundtrip: got %d bytes, want %d", len(got), size)
	}
}

func TestHeadAndExists(t *testing.T) {
	st := newTestStore(t)
	key := prefix() + "head.bin"
	body := []byte("hello head")
	if err := st.Put(context.Background(), key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	exists, size, err := st.Head(context.Background(), key)
	if err != nil || !exists {
		t.Fatalf("Head: exists=%v err=%v", exists, err)
	}
	if size != int64(len(body)) {
		t.Errorf("Head size = %d, want %d", size, len(body))
	}
	exists, _, err = st.Head(context.Background(), prefix()+"nope")
	if err != nil {
		t.Fatalf("Head(missing): %v", err)
	}
	if exists {
		t.Error("Head(missing): exists = true")
	}
}

func TestGetRangeExactBytes(t *testing.T) {
	st := newTestStore(t)
	key := prefix() + "range.bin"
	data := []byte("0123456789abcdef")
	if err := st.Put(context.Background(), key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, err := st.GetRange(context.Background(), key, 3, 9)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("345678")) {
		t.Errorf("GetRange(3,9) = %q, want %q", got, "345678")
	}
	if rrc, rerr := st.GetRange(context.Background(), key, -1, 4); rerr == nil {
		rrc.Close()
		t.Error("GetRange with negative start: want error")
	}
}

func TestUnicodeKeySegment(t *testing.T) {
	st := newTestStore(t)
	key := prefix() + "日本語-össze/τΰmƒ.pdf"
	body := []byte("unicode path")
	if err := st.Put(context.Background(), key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	rc, _, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("unicode key roundtrip mismatch: %q", got)
	}
	exists, _, err := st.Head(context.Background(), key)
	if err != nil || !exists {
		t.Errorf("Head(unicode): exists=%v err=%v", exists, err)
	}
}
