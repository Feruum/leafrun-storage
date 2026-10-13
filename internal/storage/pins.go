package storage

import (
	"context"
	"github.com/jackc/pgx/v5"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"time"
)

func retentionDeadline(class v.ObjectClass, policy string, now, hard time.Time) time.Time {
	if class == v.ClassResult || class == v.ClassRecovery {
		return hard
	}
	return now.Add(30 * 24 * time.Hour)
}
func pin(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, pid string) (v.Pin, error) {
	var p v.Pin
	e := q.QueryRow(ctx, "SELECT p.id,o.workspace,o.project,p.object_id,p.holder_id,p.holder_domain,p.policy,p.state,p.created_at,p.expires_at FROM storage_pins p JOIN storage_objects o ON o.id=p.object_id WHERE p.id=$1", pid).Scan(&p.PinID, &p.Scope.WorkspaceID, &p.Scope.ProjectID, &p.ObjectID, &p.HolderID, &p.HolderDomain, &p.RetentionPolicy, &p.State, &p.CreatedAt, &p.ExpiresAt)
	if e == pgx.ErrNoRows {
		return p, fail(404, "not_found")
	}
	return p, e
}
func holder(p v.Principal, x v.Pin) error {
	if p.Domain != x.HolderDomain || p.Subject != x.HolderID {
		return fail(404, "not_found")
	}
	return nil
}
func (s *Service) GetPin(ctx context.Context, p v.Principal, pid string) (v.Pin, error) {
	out, e := pin(ctx, s.DB.Pool, pid)
	if e != nil {
		return out, e
	}
	if e = holder(p, out); e != nil {
		return v.Pin{}, e
	}
	if e = s.authorize(ctx, p, out.Scope, "pin.get", pid); e != nil {
		return v.Pin{}, e
	}
	return out, nil
}
func (s *Service) AcquirePin(ctx context.Context, p v.Principal, in v.AcquirePinRequest) (v.Pin, error) {
	var out v.Pin
	if in.HolderDomain != p.Domain || in.HolderID != p.Subject {
		return out, fail(403, "holder_mismatch")
	}
	if in.RetentionPolicy != "ordinary" && in.RetentionPolicy != "release" && in.RetentionPolicy != "recovery" {
		return out, fail(400, "invalid_retention")
	}
	if e := s.authorize(ctx, p, in.Scope, "pin.acquire", in.ObjectID); e != nil {
		return out, e
	}
	e := s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		o, e := getObject(ctx, t, in.ObjectID)
		if e != nil {
			return e
		}
		if o.Scope != in.Scope {
			return fail(404, "not_found")
		}
		if e = ready(o, now); e != nil {
			return e
		}
		old, e := receipt(ctx, t, p, "pin", in.IdempotencyKey, in)
		if e != nil {
			return e
		}
		if old != "" {
			out, e = pin(ctx, t, old)
			if e == nil && out.State != "released" && !now.Before(out.ExpiresAt) {
				return fail(409, "pin_expired")
			}
			return e
		}
		o, e = getObject(ctx, t, in.ObjectID)
		if e != nil {
			return e
		}
		if o.Scope != in.Scope {
			return fail(404, "not_found")
		}
		if e = ready(o, now); e != nil {
			return e
		}
		if in.RetentionPolicy == "release" && o.Class != v.ClassSnapshot {
			return fail(400, "release_requires_snapshot")
		}
		if in.RetentionPolicy == "recovery" && o.Class != v.ClassRecovery {
			return fail(400, "recovery_class_required")
		}
		expires := now.Add(s.Config.PendingLease)
		if o.Expires != nil && expires.After(*o.Expires) {
			expires = *o.Expires
		}
		pid := id()
		_, e = t.Exec(ctx, "INSERT INTO storage_pins(id,object_id,holder_domain,holder_id,policy,state,created_at,expires_at) VALUES($1,$2,$3,$4,$5,'pending',$6,$7)", pid, o.ID, p.Domain, p.Subject, in.RetentionPolicy, now, expires)
		if e != nil {
			return e
		}
		if e = saveReceipt(ctx, t, p, "pin", in.IdempotencyKey, in, pid, now); e != nil {
			return e
		}
		out, e = pin(ctx, t, pid)
		return e
	})
	return out, e
}
func (s *Service) ActivatePin(ctx context.Context, p v.Principal, pid string) (v.Pin, error) {
	out, e := s.GetPin(ctx, p, pid)
	if e != nil {
		return out, e
	}
	active := false
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		out, e = pin(ctx, t, pid)
		if e != nil {
			return e
		}
		o, e := getObject(ctx, t, out.ObjectID)
		if e != nil {
			return e
		}
		if e = ready(o, now); e != nil {
			return e
		}
		if !now.Before(out.ExpiresAt) {
			return fail(409, "pin_expired")
		}
		if out.State == "active" {
			active = true
		}
		return nil
	})
	if e != nil || active {
		return out, e
	}
	state, e := s.Owner.Pin(ctx, v.PinOwnerCheck{Scope: out.Scope, PinID: pid, ObjectID: out.ObjectID, HolderID: out.HolderID, HolderDomain: out.HolderDomain})
	if e != nil {
		return out, fail(503, "owner_unavailable")
	}
	if state == v.OwnerUnknown {
		return out, fail(503, "owner_unknown")
	}
	if state != v.OwnerReferenced {
		return out, fail(409, "owner_absent")
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		out, e = pin(ctx, t, pid)
		if e != nil {
			return e
		}
		if e = holder(p, out); e != nil {
			return e
		}
		o, e := getObject(ctx, t, out.ObjectID)
		if e != nil {
			return e
		}
		if e = ready(o, now); e != nil {
			return e
		}
		if out.State == "active" {
			return nil
		}
		if out.State != "pending" || !now.Before(out.ExpiresAt) {
			return fail(409, "pin_expired")
		}
		var hard time.Time
		if o.Expires != nil {
			hard = *o.Expires
		}
		deadline := retentionDeadline(o.Class, out.RetentionPolicy, now, hard)
		if out.RetentionPolicy == "release" {
			deadline = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
		}
		_, e = t.Exec(ctx, "UPDATE storage_pins SET state='active',expires_at=$2,activated_at=$3 WHERE id=$1", pid, deadline, now)
		if e != nil {
			return e
		}
		out, e = pin(ctx, t, pid)
		return e
	})
	return out, e
}
func (s *Service) ReleasePin(ctx context.Context, p v.Principal, pid string) error {
	out, e := s.GetPin(ctx, p, pid)
	if e != nil {
		return e
	}
	return s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		out, e = pin(ctx, t, pid)
		if e != nil {
			return e
		}
		if e = holder(p, out); e != nil {
			return e
		}
		_, e = t.Exec(ctx, "UPDATE storage_pins SET state='released' WHERE id=$1", pid)
		return e
	})
}
