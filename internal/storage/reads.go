package storage

import (
	"context"
	"github.com/jackc/pgx/v5"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"sync"
	"time"
)

type leasedReader struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	release func()
	once    sync.Once
	ctx     context.Context
}

func (r *leasedReader) Deadline() (time.Time, bool) { return r.ctx.Deadline() }

func (r *leasedReader) Read(b []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		r.Close()
		return 0, e
	}
	n, e := r.body.Read(b)
	if e != nil {
		r.Close()
	}
	return n, e
}
func (r *leasedReader) Close() error {
	var e error
	r.once.Do(func() { r.cancel(); e = r.body.Close(); r.release() })
	return e
}
func (s *Service) OpenRead(ctx context.Context, p v.Principal, oid string, result bool) (io.ReadCloser, object, time.Duration, error) {
	o, e := getObject(ctx, s.DB.Pool, oid)
	if e != nil {
		return nil, o, 0, e
	}
	if result {
		a, e := artifact(ctx, s.DB.Pool, oid)
		if e != nil {
			return nil, o, 0, e
		}
		if e = s.publication(ctx, p, a); e != nil {
			return nil, o, 0, e
		}
	} else {
		if o.Class != v.ClassResource && o.Class != v.ClassRecovery {
			return nil, o, 0, fail(404, "not_found")
		}
		if e = s.authorize(ctx, p, o.Scope, "resource.read", oid); e != nil {
			return nil, o, 0, e
		}
	}
	lid := id()
	var duration time.Duration
	leaseStart := time.Now()
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		o, e = getObject(ctx, t, oid)
		if e != nil {
			return e
		}
		if e = ready(o, now); e != nil {
			return e
		}
		deadline := now.Add(s.Config.ReadLease)
		if o.Expires != nil && deadline.After(*o.Expires) {
			deadline = *o.Expires
		}
		duration = deadline.Sub(now)
		_, e = t.Exec(ctx, "INSERT INTO storage_read_leases VALUES($1,$2,$3)", lid, oid, deadline)
		return e
	})
	if e != nil {
		return nil, o, 0, e
	}
	release := func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = s.DB.Pool.Exec(c, "DELETE FROM storage_read_leases WHERE id=$1", lid)
	}
	readctx, cancel := context.WithDeadline(ctx, leaseStart.Add(duration))
	if e = readctx.Err(); e != nil {
		cancel()
		release()
		return nil, o, 0, e
	}
	body, e := s.Blob.Read(readctx, Physical{o.Key, o.Version, o.Hash, o.Size})
	if e != nil {
		cancel()
		release()
		return nil, o, 0, e
	}
	r := &leasedReader{body: body, cancel: cancel, release: release, ctx: readctx}
	go func() { <-readctx.Done(); r.Close() }()
	return r, o, duration, nil
}
