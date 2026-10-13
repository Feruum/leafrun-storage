package storage

import (
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"strings"
	"testing"
)

func TestConfigFailsClosed(t *testing.T) {
	configFixture(t)
	t.Setenv("LEAFRUN_STORAGE_SERVICE_CREDENTIALS", "")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("unset auth must fail closed")
	}
}
func TestSnapshotBindingValidation(t *testing.T) {
	m := v.SnapshotManifest{FormatVersion: 1, Files: []v.SnapshotFile{{Path: "main.typ", SHA256: strings.Repeat("a", 64), Size: 1}}, Directories: []string{}, Fonts: []v.SnapshotFont{}}
	m.ID, _ = v.ManifestContentID(m)
	for _, refs := range [][]v.ResourceRef{nil, {{Tree: "project", Path: "../main.typ", ResourceID: "r"}}, {{Tree: "other", Path: "main.typ", ResourceID: "r"}}} {
		if _, err := bindings(m, refs); err == nil {
			t.Fatal("invalid bindings accepted")
		}
	}
	if _, err := bindings(m, []v.ResourceRef{{Tree: "project", Path: "main.typ", ResourceID: "r"}}); err != nil {
		t.Fatal(err)
	}
}
