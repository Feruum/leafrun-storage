package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lostBlob struct {
	Blob
	lose bool
}

type unreadBody struct{ reads int }

func (r *unreadBody) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("completed upload replay consumed request body")
}

func (b *lostBlob) Put(ctx context.Context, key string, f *os.File, h string, n int64) (Physical, error) {
	p, e := b.Blob.Put(ctx, key, f, h, n)
	if e == nil && b.lose {
		b.lose = false
		return Physical{}, errors.New("injected lost S3 response after persisted write")
	}
	return p, e
}
func TestIntegrationLostS3AckDoesNotCreateSecondVersion(t *testing.T) {
	s, p := integration(t)
	b := &lostBlob{Blob: s.Blob, lose: true}
	s.Blob = b
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	body := "lost response"
	u, e := s.CreateUpload(context.Background(), p, v.CreateUploadRequest{Scope: scope, Class: v.ClassResource, SHA256: fingerprintBytes(body), SizeBytes: int64(len(body)), IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Put(context.Background(), p, u.UploadID, u.WriterToken, strings.NewReader(body))
	if e == nil {
		t.Fatal("lost response not exercised")
	}
	retry, e := s.Put(context.Background(), p, u.UploadID, u.WriterToken, strings.NewReader(body))
	if e != nil || retry.State != "uploaded" {
		t.Fatal("HEAD did not recover lost response", e)
	}
	physical := b.Blob.(*S3)
	versions, e := physical.Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: &physical.Bucket, Prefix: aws.String("storage/resource/uploads/" + u.UploadID)})
	if e != nil || len(versions.Versions) != 1 {
		t.Fatal("ambiguous PUT created versions", e, len(versions.Versions))
	}
	if _, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", u.UploadID); e != nil {
		t.Fatal(e)
	}
	replayBody := &unreadBody{}
	replay, e := s.Put(context.Background(), p, u.UploadID, u.WriterToken, replayBody)
	if e != nil || replay.State != "uploaded" || replayBody.reads != 0 {
		t.Fatal("expired completed upload did not reconcile without admitting bytes", replay.State, replayBody.reads, e)
	}
}
func TestIntegrationCatalogRefAndPinGCRaces(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	for range 4 {
		r := committed(t, s, p, scope, id())
		_, e := s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '2 days' WHERE id=$1", r.ResourceID)
		if e != nil {
			t.Fatal(e)
		}
		var got v.Pin
		var pinErr, gcErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			got, pinErr = s.AcquirePin(context.Background(), p, v.AcquirePinRequest{Scope: scope, ObjectID: r.ResourceID, HolderID: p.Subject, HolderDomain: p.Domain, RetentionPolicy: "ordinary", IdempotencyKey: id()})
		}()
		go func() { defer wg.Done(); gcErr = s.GC(context.Background()) }()
		wg.Wait()
		if gcErr != nil {
			t.Fatal(gcErr)
		}
		o, e := getObject(context.Background(), s.DB.Pool, r.ResourceID)
		if e != nil {
			t.Fatal(e)
		}
		if pinErr == nil && (got.State != "pending" || o.State != "ready") {
			t.Fatal("acquire lost protection race", o.State)
		}
		if pinErr != nil {
			wantStatus(t, pinErr, 409)
		}
	}
	r := committed(t, s, p, scope, "snapshot-race")
	_, e := s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '2 days' WHERE id=$1", r.ResourceID)
	if e != nil {
		t.Fatal(e)
	}
	m := v.SnapshotManifest{FormatVersion: 1, Files: []v.SnapshotFile{{Path: "a.typ", SHA256: r.SHA256, Size: r.SizeBytes}}, Directories: []string{}, Fonts: []v.SnapshotFont{}}
	m.ID, _ = v.ManifestContentID(m)
	var x v.Snapshot
	var createErr, gcErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		x, createErr = s.CreateSnapshot(context.Background(), p, v.CreateSnapshotRequest{Scope: scope, Manifest: m, Resources: []v.ResourceRef{{Tree: "project", Path: "a.typ", ResourceID: r.ResourceID}}, IdempotencyKey: id()})
	}()
	go func() { defer wg.Done(); gcErr = s.GC(context.Background()) }()
	wg.Wait()
	if gcErr != nil {
		t.Fatal(gcErr)
	}
	o, e := getObject(context.Background(), s.DB.Pool, r.ResourceID)
	if e != nil {
		t.Fatal(e)
	}
	if createErr == nil && (x.SnapshotID == "" || o.State != "ready") {
		t.Fatal("snapshot created against collected child")
	}
	if createErr != nil {
		wantStatus(t, createErr, 409)
	}
}
func TestIntegrationInventoryExactVersionsAndMultipart(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	r := committed(t, s, p, scope, "live-version")
	o, e := getObject(context.Background(), s.DB.Pool, r.ResourceID)
	if e != nil {
		t.Fatal(e)
	}
	b := s.Blob.(*S3)
	extra, e := b.Client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: &b.Bucket, Key: &o.Key, Body: strings.NewReader("extra-orphan")})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_physical_versions SET created_at=clock_timestamp()-interval '2 days' WHERE physical_key=$1 AND version=$2", o.Key, aws.ToString(extra.VersionId))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	_, e = b.Client.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &b.Bucket, Key: &o.Key, VersionId: extra.VersionId})
	if !missing(e) {
		t.Fatal("extra version was not collected", e)
	}
	body, e := b.Read(context.Background(), Physical{o.Key, o.Version, o.Hash, o.Size})
	if e != nil {
		t.Fatal("known live version deleted", e)
	}
	data, e := io.ReadAll(body)
	body.Close()
	if e != nil || string(data) != "live-version" {
		t.Fatal("exact original changed")
	}
	key := "storage/resource/uploads/orphan-" + id()
	u, e := b.Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: &b.Bucket, Key: &key})
	if e != nil {
		t.Fatal(e)
	}
	part := int32(1)
	_, e = b.Client.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: &b.Bucket, Key: &key, UploadId: u.UploadId, PartNumber: &part, Body: strings.NewReader("incomplete")})
	if e != nil {
		t.Fatal(e)
	}
	if e = b.AbortOldMultipart(context.Background(), map[string]bool{key: true}, time.Now().Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	parts, e := b.Client.ListParts(context.Background(), &s3.ListPartsInput{Bucket: &b.Bucket, Key: &key, UploadId: u.UploadId})
	if e != nil || len(parts.Parts) != 1 {
		t.Fatal("live writer multipart aborted", e)
	}
	if e = b.AbortOldMultipart(context.Background(), map[string]bool{}, time.Now().Add(time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e = b.VerifyCapabilities(context.Background()); e != nil {
		t.Fatal(e)
	}
}

// This proxy drops PostgreSQL's successful COMMIT response, after the server has
// committed. The service therefore sees a real driver failure with durable state.
func commitProxy(t *testing.T, upstream string) (string, *atomic.Bool) {
	t.Helper()
	u, e := url.Parse(upstream)
	if e != nil {
		t.Fatal(e)
	}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { listener.Close() })
	arm := &atomic.Bool{}
	upstreamHost := u.Host
	go func() {
		for {
			front, e := listener.Accept()
			if e != nil {
				return
			}
			back, e := net.Dial("tcp", upstreamHost)
			if e != nil {
				front.Close()
				continue
			}
			go func() {
				defer front.Close()
				defer back.Close()
				go io.Copy(back, front)
				for {
					h := make([]byte, 5)
					if _, e := io.ReadFull(back, h); e != nil {
						return
					}
					n := int(binary.BigEndian.Uint32(h[1:]))
					if n < 4 || n > 32<<20 {
						return
					}
					payload := make([]byte, n-4)
					if _, e := io.ReadFull(back, payload); e != nil {
						return
					}
					if h[0] == 'C' && string(payload) == "COMMIT\x00" && arm.CompareAndSwap(true, false) {
						return
					}
					if _, e := front.Write(h); e != nil {
						return
					}
					if _, e := front.Write(payload); e != nil {
						return
					}
				}
			}()
		}
	}()
	u.Host = listener.Addr().String()
	return u.String(), arm
}
func TestIntegrationLostPostgresCommitResponse(t *testing.T) {
	s, p := integration(t)
	proxied, arm := commitProxy(t, s.Config.DatabaseURL)
	d, e := OpenDB(context.Background(), proxied)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(d.Pool.Close)
	s.DB = d
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, p, scope)
	u := uploaded(t, s, p, scope, v.ClassResult, "durable-result")
	in := v.FinalizeArtifactRequest{WriterToken: u.WriterToken, SnapshotID: snapshot.SnapshotID, OriginJobID: id(), Metadata: v.ArtifactMetadata{Filename: "x.pdf", MediaType: "application/pdf", TargetID: "t", AttemptID: "a"}, IdempotencyKey: id()}
	arm.Store(true)
	_, e = s.Finalize(context.Background(), p, u.UploadID, in)
	if e == nil {
		t.Fatal("COMMIT response was not dropped")
	}
	receipt, e := s.GetUpload(context.Background(), p, u.UploadID)
	if e != nil || receipt.ArtifactID == "" {
		t.Fatal("durable receipt missing after lost COMMIT", e)
	}
	_, e = d.Pool.Exec(context.Background(), "UPDATE storage_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", u.UploadID)
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.Finalize(context.Background(), p, u.UploadID, in)
	if e != nil || a.ArtifactID != receipt.ArtifactID {
		t.Fatal("lost DB ack not recovered", e)
	}
}
