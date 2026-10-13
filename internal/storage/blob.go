package storage

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"io"
	"os"
	"strings"
	"time"
)

type Physical struct {
	Key, Version, Hash string
	Size               int64
}

func (p Physical) exact() bool { return p.Key != "" && p.Version != "" }

type Blob interface {
	Probe(context.Context) error
	Find(context.Context, string) (Physical, bool, error)
	Put(context.Context, string, *os.File, string, int64) (Physical, error)
	Read(context.Context, Physical) (io.ReadCloser, error)
	Delete(context.Context, Physical) error
	Inventory(context.Context, string, string) ([]Physical, string, string, error)
	AbortOldMultipart(context.Context, map[string]bool, time.Time) error
}
type S3 struct {
	Client *s3.Client
	Bucket string
}

func OpenS3(ctx context.Context, c Config) (*S3, error) {
	a, e := awsconfig.LoadDefaultConfig(ctx)
	if e != nil {
		return nil, e
	}
	client := s3.NewFromConfig(a, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &S3{client, c.Bucket}, nil
}
func (s *S3) Probe(ctx context.Context) error {
	_, e := s.Client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &s.Bucket})
	if e != nil {
		return e
	}
	v, e := s.Client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: &s.Bucket})
	if e != nil {
		return e
	}
	if v.Status != types.BucketVersioningStatusEnabled {
		return errors.New("S3 bucket versioning must be enabled")
	}
	_, e = s.Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: &s.Bucket, Prefix: aws.String("storage/"), MaxKeys: aws.Int32(1)})
	if e != nil {
		return e
	}
	_, e = s.Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &s.Bucket, Prefix: aws.String("storage/"), MaxUploads: aws.Int32(1)})
	return e
}

// VerifyCapabilities checks exact-version and multipart behavior on private probe keys.
func (s *S3) VerifyCapabilities(ctx context.Context) error {
	if e := s.Probe(ctx); e != nil {
		return e
	}
	key := "storage/probes/" + id()
	r, e := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.Bucket, Key: &key, Body: strings.NewReader("probe"), IfNoneMatch: aws.String("*")})
	if e != nil {
		return e
	}
	p := Physical{Key: key, Version: aws.ToString(r.VersionId), Size: 5}
	if !p.exact() {
		return errors.New("S3 did not return exact version")
	}
	body, e := s.Read(ctx, p)
	if e != nil {
		return e
	}
	data, e := io.ReadAll(body)
	body.Close()
	if e != nil {
		return e
	}
	if string(data) != "probe" {
		return errors.New("S3 exact version roundtrip mismatch")
	}
	if e = s.Delete(ctx, p); e != nil {
		return e
	}
	key = "storage/probes/" + id()
	upload, e := s.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &s.Bucket, Key: &key})
	if e != nil {
		return e
	}
	_, e = s.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.Bucket, Key: &key, UploadId: upload.UploadId})
	if e != nil {
		return e
	}
	_, e = s.Client.ListParts(ctx, &s3.ListPartsInput{Bucket: &s.Bucket, Key: &key, UploadId: upload.UploadId})
	var api smithy.APIError
	if e == nil || !errors.As(e, &api) || api.ErrorCode() != "NoSuchUpload" {
		return errors.New("S3 multipart abort not confirmed")
	}
	return nil
}
func missing(e error) bool {
	var a smithy.APIError
	return errors.As(e, &a) && (a.ErrorCode() == "NotFound" || a.ErrorCode() == "NoSuchKey" || a.ErrorCode() == "NoSuchVersion")
}
func (s *S3) Find(ctx context.Context, key string) (Physical, bool, error) {
	h, e := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.Bucket, Key: &key})
	if missing(e) {
		return Physical{}, false, nil
	}
	if e != nil {
		return Physical{}, false, e
	}
	p := Physical{key, aws.ToString(h.VersionId), h.Metadata["sha256"], aws.ToInt64(h.ContentLength)}
	if !p.exact() {
		return p, false, errors.New("S3 missing version")
	}
	return p, true, nil
}
func (s *S3) Put(ctx context.Context, key string, f *os.File, hash string, size int64) (Physical, error) {
	_, e := f.Seek(0, 0)
	if e != nil {
		return Physical{}, e
	}
	digest, _ := hex.DecodeString(hash)
	r, e := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.Bucket, Key: &key, Body: f, ContentLength: &size, IfNoneMatch: aws.String("*"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest)), Metadata: map[string]string{"sha256": hash}})
	if e != nil {
		p, ok, headErr := s.Find(ctx, key)
		if headErr == nil && ok && p.Hash == hash && p.Size == size {
			return p, nil
		}
		return Physical{}, e
	}
	p := Physical{key, aws.ToString(r.VersionId), hash, size}
	if !p.exact() {
		return p, errors.New("S3 missing version")
	}
	return p, nil
}
func (s *S3) Read(ctx context.Context, p Physical) (io.ReadCloser, error) {
	if !p.exact() {
		return nil, errors.New("exact S3 receipt required")
	}
	in := &s3.GetObjectInput{Bucket: &s.Bucket, Key: &p.Key}
	if p.Version != "" {
		in.VersionId = &p.Version
	}
	r, e := s.Client.GetObject(ctx, in)
	if e != nil {
		return nil, e
	}
	return r.Body, nil
}
func (s *S3) Delete(ctx context.Context, p Physical) error {
	if !p.exact() {
		return errors.New("exact S3 receipt required")
	}
	in := &s3.DeleteObjectInput{Bucket: &s.Bucket, Key: &p.Key}
	if p.Version != "" {
		in.VersionId = &p.Version
	}
	_, e := s.Client.DeleteObject(ctx, in)
	if e != nil {
		return e
	}
	hi := &s3.HeadObjectInput{Bucket: &s.Bucket, Key: &p.Key}
	if p.Version != "" {
		hi.VersionId = &p.Version
	}
	_, e = s.Client.HeadObject(ctx, hi)
	if missing(e) {
		return nil
	}
	if e == nil {
		return errors.New("physical version remains after delete")
	}
	return e
}
func (s *S3) Inventory(ctx context.Context, key, version string) ([]Physical, string, string, error) {
	out := []Physical{}
	in := &s3.ListObjectVersionsInput{Bucket: &s.Bucket, Prefix: aws.String("storage/"), MaxKeys: aws.Int32(500)}
	if key != "" {
		in.KeyMarker = &key
	}
	if version != "" {
		in.VersionIdMarker = &version
	}
	p, e := s.Client.ListObjectVersions(ctx, in)
	if e != nil {
		return nil, "", "", e
	}
	for _, x := range p.Versions {
		out = append(out, Physical{Key: aws.ToString(x.Key), Version: aws.ToString(x.VersionId), Size: aws.ToInt64(x.Size)})
	}
	for _, x := range p.DeleteMarkers {
		out = append(out, Physical{Key: aws.ToString(x.Key), Version: aws.ToString(x.VersionId)})
	}
	if aws.ToBool(p.IsTruncated) {
		return out, aws.ToString(p.NextKeyMarker), aws.ToString(p.NextVersionIdMarker), nil
	}
	return out, "", "", nil
}
func (s *S3) AbortOldMultipart(ctx context.Context, live map[string]bool, cutoff time.Time) error {
	pager := s3.NewListMultipartUploadsPaginator(s.Client, &s3.ListMultipartUploadsInput{Bucket: &s.Bucket, Prefix: aws.String("storage/")})
	for pager.HasMorePages() {
		p, e := pager.NextPage(ctx)
		if e != nil {
			return e
		}
		for _, u := range p.Uploads {
			if live[aws.ToString(u.Key)] || u.Initiated == nil || !u.Initiated.Before(cutoff) {
				continue
			}
			_, e = s.Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: &s.Bucket, Key: u.Key, UploadId: u.UploadId})
			if e != nil {
				return e
			}
			_, e = s.Client.ListParts(ctx, &s3.ListPartsInput{Bucket: &s.Bucket, Key: u.Key, UploadId: u.UploadId})
			var api smithy.APIError
			if e == nil {
				return errors.New("multipart still present")
			}
			if !errors.As(e, &api) || api.ErrorCode() != "NoSuchUpload" {
				return e
			}
		}
	}
	return nil
}
