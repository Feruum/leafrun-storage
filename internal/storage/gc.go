package storage

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

var gcDeletedBytes, gcErrors, gcOrphans, gcUnresolved atomic.Int64

func (s *Service) metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var lag float64
	e := s.DB.Pool.QueryRow(r.Context(), "SELECT COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-MIN(expires_at)),0)::float8 FROM storage_objects WHERE state<>'deleted' AND expires_at<clock_timestamp()").Scan(&lag)
	if e != nil {
		lag = 0
	}
	fmt.Fprintf(w, "leafrun_storage_gc_lag_seconds %g\nleafrun_storage_gc_deleted_bytes_total %d\nleafrun_storage_gc_errors_total %d\nleafrun_storage_gc_orphans_total %d\nleafrun_storage_gc_unresolved_pins %d\n", lag, gcDeletedBytes.Load(), gcErrors.Load(), gcOrphans.Load(), gcUnresolved.Load())
}

// GC uses a session advisory lock, in addition to the short catalog transaction lock.
// External calls never execute while a catalog transaction is open.
func (s *Service) GC(ctx context.Context) error {
	conn, e := s.DB.Pool.Acquire(ctx)
	if e != nil {
		return e
	}
	defer conn.Release()
	var locked bool
	e = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(710033)").Scan(&locked)
	if e != nil || !locked {
		return e
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock(710033)")
	if e = s.reconcilePins(ctx); e != nil {
		return e
	}
	var objects []object
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		_, e := t.Exec(ctx, "DELETE FROM storage_read_leases WHERE expires_at<=$1", now)
		if e != nil {
			return e
		}
		rows, e := t.Query(ctx, `SELECT o.id FROM storage_objects o WHERE o.state='deleting' OR (o.state<>'deleted' AND NOT EXISTS(SELECT 1 FROM storage_read_leases l WHERE l.object_id=o.id AND l.expires_at>$1) AND ((o.expires_at IS NOT NULL AND o.expires_at<=$1) OR (o.created_at<=$2 AND NOT EXISTS(SELECT 1 FROM storage_uploads u WHERE u.object_id=o.id AND u.state IN ('created','writing','uploaded') AND u.lease_until>$1) AND NOT EXISTS(SELECT 1 FROM storage_pins p WHERE p.object_id=o.id AND (p.state='unresolved' OR p.state='pending' OR (p.state='active' AND p.expires_at>$1))) AND NOT EXISTS(SELECT 1 FROM storage_snapshot_refs r JOIN storage_objects sn ON sn.id=r.snapshot_id WHERE r.resource_id=o.id AND sn.state<>'deleted') AND NOT EXISTS(SELECT 1 FROM storage_artifacts a JOIN storage_objects ar ON ar.id=a.id WHERE a.snapshot_id=o.id AND ar.state='ready' AND ar.expires_at>$1)))) ORDER BY o.created_at LIMIT 100`, now, now.Add(-s.Config.OrphanGrace))
		if e != nil {
			return e
		}
		ids := []string{}
		for rows.Next() {
			var oid string
			if e = rows.Scan(&oid); e != nil {
				rows.Close()
				return e
			}
			ids = append(ids, oid)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for _, oid := range ids {
			o, e := getObject(ctx, t, oid)
			if e != nil {
				return e
			}
			if _, e = t.Exec(ctx, "UPDATE storage_objects SET state='deleting' WHERE id=$1", oid); e != nil {
				return e
			}
			if _, e = t.Exec(ctx, "UPDATE storage_uploads SET state='deleted' WHERE object_id=$1 AND state<>'committed'", oid); e != nil {
				return e
			}
			o.State = "deleting"
			objects = append(objects, o)
		}
		return nil
	})
	if e != nil {
		return e
	}
	for _, o := range objects {
		if o.Key != "" {
			if o.Version == "" {
				physical, exists, lookupErr := s.Blob.Find(ctx, o.Key)
				if lookupErr != nil {
					return lookupErr
				}
				if exists {
					o.Version = physical.Version
				} else {
					o.Key = ""
				}
			}
		}
		if o.Key != "" {
			var canonical bool
			if e = s.DB.Pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM storage_resources WHERE id=$1) OR EXISTS(SELECT 1 FROM storage_artifacts WHERE id=$1)", o.ID).Scan(&canonical); e != nil {
				return e
			}
			if !canonical {
				oldEnough := false
				if e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
					_, e := t.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,object_id,created_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", o.Key, o.Version, o.ID, now)
					if e != nil {
						return e
					}
					var created time.Time
					if e = t.QueryRow(ctx, "SELECT created_at FROM storage_physical_versions WHERE physical_key=$1 AND version=$2", o.Key, o.Version).Scan(&created); e != nil {
						return e
					}
					oldEnough = !now.Before(created.Add(s.Config.OrphanGrace))
					return nil
				}); e != nil {
					return e
				}
				if !oldEnough {
					continue
				}
			}
			if e = s.Blob.Delete(ctx, Physical{o.Key, o.Version, o.Hash, o.Size}); e != nil {
				gcErrors.Add(1)
				slog.Error("storage deletion retry", "objectId", o.ID, "error", e)
				continue
			}
		}
		e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
			if _, e := t.Exec(ctx, "DELETE FROM storage_snapshot_refs WHERE snapshot_id=$1", o.ID); e != nil {
				return e
			}
			if _, e := t.Exec(ctx, "UPDATE storage_objects SET state='deleted' WHERE id=$1 AND state='deleting'", o.ID); e != nil {
				return e
			}
			if _, e := t.Exec(ctx, "UPDATE storage_physical_versions SET state='deleted' WHERE physical_key=$1 AND version=$2", o.Key, o.Version); e != nil {
				return e
			}
			_, e := t.Exec(ctx, "INSERT INTO storage_deletion_history VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", o.ID, now, o.Class, o.Size)
			return e
		})
		if e != nil {
			return e
		}
		gcDeletedBytes.Add(o.Size)
		slog.Info("storage object deleted", "objectId", o.ID, "class", o.Class, "bytes", o.Size)
	}
	if e = s.inventory(ctx); e != nil {
		gcErrors.Add(1)
		return e
	}
	if e = s.janitor(ctx); e != nil {
		return e
	}
	return s.purgeHistory(ctx)
}
func (s *Service) reconcilePins(ctx context.Context) error {
	rows, e := s.DB.Pool.Query(ctx, "SELECT id FROM storage_pins WHERE state IN ('pending','unresolved','active') AND expires_at<=clock_timestamp() ORDER BY last_reconciled_at NULLS FIRST,expires_at,id LIMIT 100")
	if e != nil {
		return e
	}
	ids := []string{}
	for rows.Next() {
		var pid string
		if e = rows.Scan(&pid); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, pid)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	// Claim the batch before owner I/O. Even a crash or activity deadline cannot
	// repeatedly leave the same unknown owners ahead of all unattempted pins.
	if e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		_, e := t.Exec(ctx, "UPDATE storage_pins SET last_reconciled_at=$2 WHERE id=ANY($1) AND state IN ('pending','unresolved','active') AND expires_at<=$2", ids, now)
		return e
	}); e != nil {
		return e
	}
	for _, pid := range ids {
		p, e := pin(ctx, s.DB.Pool, pid)
		if e != nil {
			return e
		}
		state, ownerErr := s.Owner.Pin(ctx, v.PinOwnerCheck{Scope: p.Scope, PinID: p.PinID, ObjectID: p.ObjectID, HolderID: p.HolderID, HolderDomain: p.HolderDomain})
		next := "unresolved"
		if ownerErr == nil && state == v.OwnerAbsent {
			next = "released"
		}
		e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
			current, e := pin(ctx, t, pid)
			if e != nil {
				return e
			}
			if current.State == "released" || current.State == "active" && current.ExpiresAt.After(now) {
				return nil
			}
			o, e := getObject(ctx, t, current.ObjectID)
			if e != nil {
				return e
			}
			if o.Expires != nil && !now.Before(*o.Expires) {
				next = "released"
			}
			_, e = t.Exec(ctx, "UPDATE storage_pins SET state=$2,last_reconciled_at=$3 WHERE id=$1", pid, next, now)
			return e
		})
		if e != nil {
			return e
		}
	}
	var unresolved int64
	if e = s.DB.Pool.QueryRow(ctx, "SELECT count(*) FROM storage_pins WHERE state='unresolved'").Scan(&unresolved); e != nil {
		return e
	}
	gcUnresolved.Store(unresolved)
	return nil
}
func (s *Service) inventory(ctx context.Context) error {
	var cursorKey, cursorVersion string
	if e := s.DB.Pool.QueryRow(ctx, "SELECT physical_key,version FROM storage_gc_cursor WHERE id=1").Scan(&cursorKey, &cursorVersion); e != nil {
		return e
	}
	items, nextKey, nextVersion, e := s.Blob.Inventory(ctx, cursorKey, cursorVersion)
	if e != nil {
		return e
	}
	live := map[string]bool{}
	rows, e := s.DB.Pool.Query(ctx, "SELECT physical_key FROM storage_objects o WHERE physical_key<>'' AND EXISTS(SELECT 1 FROM storage_uploads u WHERE u.object_id=o.id AND u.lease_until>clock_timestamp() AND u.state IN ('created','writing','uploaded'))")
	if e != nil {
		return e
	}
	for rows.Next() {
		var key string
		if e = rows.Scan(&key); e != nil {
			rows.Close()
			return e
		}
		live[key] = true
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, item := range items {
		if live[item.Key] {
			continue
		}
		eligible := false
		e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
			_, e := t.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,created_at) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", item.Key, item.Version, now)
			if e != nil {
				return e
			}
			var first time.Time
			var state string
			e = t.QueryRow(ctx, "SELECT created_at,state FROM storage_physical_versions WHERE physical_key=$1 AND version=$2", item.Key, item.Version).Scan(&first, &state)
			if e != nil {
				return e
			}
			var protected bool
			e = t.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM storage_objects o WHERE physical_key=$1 AND ((state='ready' AND version=$2) OR EXISTS(SELECT 1 FROM storage_uploads u WHERE u.object_id=o.id AND u.lease_until>$3 AND u.state IN ('created','writing','uploaded'))))`, item.Key, item.Version, now).Scan(&protected)
			if e != nil {
				return e
			}
			eligible = !protected && state != "deleted" && !now.Before(first.Add(s.Config.OrphanGrace))
			return nil
		})
		if e != nil {
			return e
		}
		if !eligible {
			continue
		}
		if e = s.Blob.Delete(ctx, item); e != nil {
			return e
		}
		gcOrphans.Add(1)
		_, e = s.DB.Pool.Exec(ctx, "UPDATE storage_physical_versions SET state='deleted' WHERE physical_key=$1 AND version=$2", item.Key, item.Version)
		if e != nil {
			return e
		}
	}
	if _, e = s.DB.Pool.Exec(ctx, "UPDATE storage_gc_cursor SET physical_key=$1,version=$2 WHERE id=1", nextKey, nextVersion); e != nil {
		return e
	}
	var now time.Time
	if e = s.DB.Pool.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return e
	}
	return s.Blob.AbortOldMultipart(ctx, live, now.Add(-s.Config.OrphanGrace))
}
func (s *Service) purgeHistory(ctx context.Context) error {
	return s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		cutoff := now.Add(-30 * 24 * time.Hour)
		// Remove only catalog rows with confirmed deletion history. Live pins and references
		// are independent of the history TTL and are never purged by age.
		rows, e := t.Query(ctx, `SELECT o.id FROM storage_objects o JOIN storage_deletion_history h ON h.id=o.id WHERE o.state='deleted' AND h.deleted_at<$1 AND NOT EXISTS(SELECT 1 FROM storage_snapshot_refs r WHERE r.resource_id=o.id) AND NOT EXISTS(SELECT 1 FROM storage_artifacts a WHERE a.snapshot_id=o.id) LIMIT 100`, cutoff)
		if e != nil {
			return e
		}
		ids := []string{}
		for rows.Next() {
			var oid string
			if e = rows.Scan(&oid); e != nil {
				rows.Close()
				return e
			}
			ids = append(ids, oid)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for _, oid := range ids {
			for _, query := range []string{"DELETE FROM storage_idempotency WHERE object_id=$1 OR operation IN ('commit:'||$1,'finalize:'||$1)", "DELETE FROM storage_idempotency WHERE object_id IN(SELECT id FROM storage_pins WHERE object_id=$1)", "DELETE FROM storage_pins WHERE object_id=$1", "DELETE FROM storage_read_leases WHERE object_id=$1", "DELETE FROM storage_uploads WHERE object_id=$1", "DELETE FROM storage_artifacts WHERE id=$1", "DELETE FROM storage_snapshots WHERE id=$1", "DELETE FROM storage_resources WHERE id=$1", "DELETE FROM storage_objects WHERE id=$1", "DELETE FROM storage_deletion_history WHERE id=$1"} {
				if _, e = t.Exec(ctx, query, oid); e != nil {
					return e
				}
			}
		}
		_, e = t.Exec(ctx, "DELETE FROM storage_physical_versions WHERE state='deleted' AND created_at<$1", cutoff)
		return e
	})
}
