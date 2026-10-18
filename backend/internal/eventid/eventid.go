// Package eventid derives deterministic event IDs: the same logical fact
// always gets the same ID, so resends after a crash, a replay, or a CDC
// redelivery are recognised as duplicates by consumers.
package eventid

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// New returns 32 hex chars (128 bits); also valid as a PostgreSQL uuid literal.
func New(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(h[:16])
}
