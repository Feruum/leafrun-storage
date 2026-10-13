package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIntegrationHTTPContractRoutes(t *testing.T) {
	s, _ := integration(t)
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	client, e := v.NewClient(server.URL, s.Config.Credentials[0].Token, nil)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	u, e := client.CreateUpload(ctx, v.CreateUploadRequest{Scope: scope, Class: v.ClassResource, SHA256: fingerprintBytes("main"), SizeBytes: 4, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = client.RenewUpload(ctx, u.UploadID, v.RenewUploadRequest{WriterToken: u.WriterToken}); e != nil {
		t.Fatal(e)
	}
	if _, e = client.PutUpload(ctx, u.UploadID, u.WriterToken, strings.NewReader("main")); e != nil {
		t.Fatal(e)
	}
	r, e := client.CommitResource(ctx, u.UploadID, v.CommitResourceRequest{WriterToken: u.WriterToken, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	content, e := client.ResourceContent(ctx, r.ResourceID)
	if e != nil {
		t.Fatal(e)
	}
	data, e := io.ReadAll(content.Body)
	content.Body.Close()
	if e != nil || string(data) != "main" {
		t.Fatal("resource stream mismatch", e)
	}
	m := v.SnapshotManifest{FormatVersion: 1, Files: []v.SnapshotFile{{Path: "main.typ", SHA256: r.SHA256, Size: 4}}, Directories: []string{}, Fonts: []v.SnapshotFont{}}
	m.ID, _ = v.ManifestContentID(m)
	snapshot, e := client.CreateSnapshot(ctx, v.CreateSnapshotRequest{Scope: scope, Manifest: m, Resources: []v.ResourceRef{{Tree: "project", Path: "main.typ", ResourceID: r.ResourceID}}, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = client.GetSnapshot(ctx, snapshot.SnapshotID); e != nil {
		t.Fatal(e)
	}
	result, e := client.CreateUpload(ctx, v.CreateUploadRequest{Scope: scope, Class: v.ClassResult, SHA256: fingerprintBytes("pdf"), SizeBytes: 3, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = client.PutUpload(ctx, result.UploadID, result.WriterToken, strings.NewReader("pdf")); e != nil {
		t.Fatal(e)
	}
	a, e := client.FinalizeArtifact(ctx, result.UploadID, v.FinalizeArtifactRequest{WriterToken: result.WriterToken, OriginJobID: id(), SnapshotID: snapshot.SnapshotID, Metadata: v.ArtifactMetadata{Filename: "output.pdf", MediaType: "application/pdf", TargetID: "t", AttemptID: "a"}, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = client.GetArtifact(ctx, a.ArtifactID); e != nil {
		t.Fatal(e)
	}
	download, e := client.ArtifactContent(ctx, a.ArtifactID)
	if e != nil {
		t.Fatal(e)
	}
	data, e = io.ReadAll(download.Body)
	download.Body.Close()
	if e != nil || string(data) != "pdf" || download.Header.Get("Content-Type") != "application/pdf" {
		t.Fatal("artifact stream mismatch", e)
	}
	pin, e := client.AcquirePin(ctx, v.AcquirePinRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderDomain: s.Config.Credentials[0].Domain, HolderID: s.Config.Credentials[0].Subject, RetentionPolicy: "release", IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = client.ActivatePin(ctx, pin.PinID); e != nil {
		t.Fatal(e)
	}
	if _, e = client.GetPin(ctx, pin.PinID); e != nil {
		t.Fatal(e)
	}
	if e = client.ReleasePin(ctx, pin.PinID); e != nil {
		t.Fatal(e)
	}
	if _, e = client.GetUpload(ctx, result.UploadID); e != nil {
		t.Fatal(e)
	}
	s.Owner.(*testOwner).publish = false
	if _, e = client.GetArtifact(ctx, a.ArtifactID); e == nil {
		t.Fatal("unpublished artifact visible")
	}
}
