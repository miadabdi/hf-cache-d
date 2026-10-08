// Package manifest defines the per-repo/commit manifests Tasks 3 and 4
// write into the store alongside cached data. This file holds only the
// shared types and hashing helpers; lane-specific usage lands with those
// tasks.
package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"time"
)

// Manifest records one pull of a repo at a commit: the repo@revision
// identity it was requested under, the files cached by that pull (path →
// sha256, with byte sizes in Sizes), when it happened, and which upstream
// served it. Upstream CDN ETags are deliberately NOT stored: they are
// volatile and sha256 is the ETag this service serves.
type Manifest struct {
	Identity string            // e.g. "org/name@main" or "org/name@<40-hex>"
	Files    map[string]string // repo-relative file path → sha256 hex
	Sizes    map[string]int64  // repo-relative file path → byte size
	PulledAt time.Time
	Upstream string // upstream base URL the data came from

	// CacheKey is the store object key this manifest was loaded from or
	// published to. It is in-memory only (never serialized: the stored JSON
	// is canonical across writers) and lets the integrity self-check verify
	// recently-served manifests without bucket listing.
	CacheKey string `json:"-"`
}

// HashReader streams r through sha256 and returns the hex digest and the
// number of bytes read. It does not buffer the input.
func HashReader(r io.Reader) (digest string, n int64, err error) {
	h := sha256.New()
	n, err = io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
