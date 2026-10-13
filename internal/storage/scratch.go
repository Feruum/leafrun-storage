package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func safeScratch(root, name string) bool {
	a, e := filepath.Abs(root)
	if e != nil {
		return false
	}
	b, e := filepath.Abs(name)
	if e != nil {
		return false
	}
	return filepath.Dir(b) == a && strings.HasPrefix(filepath.Base(b), "leafrun-storage-upload-")
}
func (s *Service) stopScratch(f *os.File, uid string) {
	name := f.Name()
	_ = f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = s.DB.Pool.Exec(ctx, "UPDATE storage_scratch SET state='stopped',stopped_at=clock_timestamp() WHERE path=$1 AND upload_id=$2", name, uid)
	if e := os.Remove(name); e == nil || errors.Is(e, os.ErrNotExist) {
		_, _ = s.DB.Pool.Exec(ctx, "DELETE FROM storage_scratch WHERE path=$1 AND state='stopped'", name)
	}
}

// Unknown/crashed writers are deliberately not removed: only a process that closed
// its file can mark it stopped. A janitor never infers safe process death from age.
func (s *Service) janitor(ctx context.Context) error {
	rows, e := s.DB.Pool.Query(ctx, `SELECT x.path FROM storage_scratch x LEFT JOIN storage_uploads u ON u.id=x.upload_id WHERE x.state='stopped' AND x.stopped_at<clock_timestamp()-interval '5 minutes' AND (u.id IS NULL OR u.lease_until<=clock_timestamp()) LIMIT 100`)
	if e != nil {
		return e
	}
	paths := []string{}
	for rows.Next() {
		var name string
		if e = rows.Scan(&name); e != nil {
			rows.Close()
			return e
		}
		paths = append(paths, name)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, name := range paths {
		if !safeScratch(s.Config.Scratch, name) {
			continue
		}
		info, e := os.Lstat(name)
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if e == nil && !info.Mode().IsRegular() {
			continue
		}
		if e = os.Remove(name); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		if _, e = s.DB.Pool.Exec(ctx, "DELETE FROM storage_scratch WHERE path=$1 AND state='stopped'", name); e != nil {
			return e
		}
	}
	return nil
}
