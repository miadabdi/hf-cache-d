package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestHashReader(t *testing.T) {
	// 1<<16+1 bytes: crosses internal copy buffer boundaries.
	data := strings.Repeat("x", 1<<16) + "!"
	want := sha256.Sum256([]byte(data))

	got, n, err := HashReader(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if got != hex.EncodeToString(want[:]) || n != int64(len(data)) {
		t.Errorf("HashReader = %s, %d; want %s, %d", got, n, hex.EncodeToString(want[:]), len(data))
	}
}
