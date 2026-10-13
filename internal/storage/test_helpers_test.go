package storage

import (
	"crypto/sha256"
	"encoding/hex"
)

func fingerprintBytes(s string) string {
	d := sha256.Sum256([]byte(s))
	return hex.EncodeToString(d[:])
}
