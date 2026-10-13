package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScratchJanitorRejectsArbitraryTrees(t *testing.T) {
	root := t.TempDir()
	for _, bad := range []string{filepath.Join(root, "worker", "tree"), filepath.Join(root, "..", "leafrun-storage-upload-escape"), filepath.Join(root, "source.typ")} {
		if safeScratch(root, bad) {
			t.Fatal("unsafe path accepted", bad)
		}
	}
	if !safeScratch(root, filepath.Join(root, "leafrun-storage-upload-safe")) {
		t.Fatal("own file rejected")
	}
	if e := os.WriteFile(filepath.Join(root, "source.typ"), []byte("keep"), 0600); e != nil {
		t.Fatal(e)
	}
}
