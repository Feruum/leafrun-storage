package storage

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func writeError(w http.ResponseWriter, e error) {
	status, code := 500, "internal_error"
	var f *fault
	if errors.As(e, &f) {
		status, code = f.Status, f.Code
	} else {
		slog.Error("storage request failed", "error", e)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v.ErrorResponse{Error: v.APIError{Code: code, Message: code}})
}
func respond(w http.ResponseWriter, x any, e error) {
	if e != nil {
		writeError(w, e)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(x)
}
func decode(r *http.Request, x any) error {
	d := json.NewDecoder(io.LimitReader(r.Body, 16<<20))
	d.DisallowUnknownFields()
	if e := d.Decode(x); e != nil {
		return fail(400, "invalid_json")
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return fail(400, "invalid_json")
	}
	return nil
}
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, map[string]string{"status": "ok"}, nil) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, c := context.WithTimeout(r.Context(), 3*time.Second)
		defer c()
		e := s.DB.Pool.Ping(ctx)
		if e == nil {
			e = s.Blob.Probe(ctx)
		}
		if e != nil {
			writeError(w, fail(503, "not_ready"))
			return
		}
		respond(w, map[string]string{"status": "ready"}, nil)
	})
	mux.HandleFunc("GET /metrics", s.metricsHandler)
	type action func(http.ResponseWriter, *http.Request, v.Principal)
	register := func(pattern string, fn action) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			var p v.Principal
			ok := false
			if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
				for _, c := range s.Config.Credentials {
					if subtle.ConstantTimeCompare([]byte(token), []byte(c.Token)) == 1 {
						p = v.Principal{Subject: c.Subject, Domain: c.Domain}
						ok = true
						break
					}
				}
			}
			if !ok {
				writeError(w, fail(401, "unauthorized"))
				return
			}
			fn(w, r, p)
		})
	}
	register("POST /internal/v1/uploads", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.CreateUploadRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.CreateUpload(r.Context(), p, in)
		respond(w, x, e)
	})
	register("GET /internal/v1/uploads/{id}", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		x, e := s.GetUpload(r.Context(), p, r.PathValue("id"))
		respond(w, x, e)
	})
	register("PUT /internal/v1/uploads/{id}/content", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		r.Body = http.MaxBytesReader(w, r.Body, s.Config.MaxUpload+1)
		controller := http.NewResponseController(w)
		x, e := s.put(r.Context(), p, r.PathValue("id"), r.Header.Get("X-Writer-Token"), r.Body, controller.SetReadDeadline)
		respond(w, x, e)
	})
	register("POST /internal/v1/uploads/{id}/renew", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.RenewUploadRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.Renew(r.Context(), p, r.PathValue("id"), in.WriterToken)
		respond(w, x, e)
	})
	register("POST /internal/v1/uploads/{id}/commit", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.CommitResourceRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.Commit(r.Context(), p, r.PathValue("id"), in)
		respond(w, x, e)
	})
	register("POST /internal/v1/uploads/{id}/finalize", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.FinalizeArtifactRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.Finalize(r.Context(), p, r.PathValue("id"), in)
		respond(w, x, e)
	})
	register("POST /internal/v1/snapshots", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.CreateSnapshotRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.CreateSnapshot(r.Context(), p, in)
		respond(w, x, e)
	})
	register("GET /internal/v1/snapshots/{id}", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		x, e := s.GetSnapshot(r.Context(), p, r.PathValue("id"))
		respond(w, x, e)
	})
	register("GET /v1/artifacts/{id}", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		x, e := s.GetArtifact(r.Context(), p, r.PathValue("id"))
		respond(w, x, e)
	})
	content := func(result bool) action {
		return func(w http.ResponseWriter, r *http.Request, p v.Principal) {
			body, o, _, e := s.OpenRead(r.Context(), p, r.PathValue("id"), result)
			if e != nil {
				writeError(w, e)
				return
			}
			defer body.Close()
			lease, ok := body.(interface{ Deadline() (time.Time, bool) })
			if !ok {
				writeError(w, fail(503, "stream_deadline_unavailable"))
				return
			}
			deadline, ok := lease.Deadline()
			if !ok {
				writeError(w, fail(503, "stream_deadline_unavailable"))
				return
			}
			controller := http.NewResponseController(w)
			if e = controller.SetWriteDeadline(deadline); e != nil {
				writeError(w, fail(503, "stream_deadline_unavailable"))
				return
			}
			// A reader cancels its own context at EOF, possibly before the final
			// buffered bytes are written. Only request cancellation forces an early
			// writer deadline; the lease deadline itself is fixed, never renewed.
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				select {
				case <-r.Context().Done():
					_ = controller.SetWriteDeadline(time.Now())
				case <-stop:
				}
			}()
			defer func() { close(stop); <-done }()
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.Header().Set("Content-Length", strconv.FormatInt(o.Size, 10))
			w.Header().Set("ETag", `"`+o.Hash+`"`)
			if result {
				a, e := artifact(r.Context(), s.DB.Pool, o.ID)
				if e != nil {
					writeError(w, e)
					return
				}
				w.Header().Set("Content-Type", a.Metadata.MediaType)
			}
			_, e = io.Copy(w, body)
			if e != nil {
				slog.Warn("storage stream interrupted", "objectId", o.ID, "error", e)
			}
		}
	}
	register("GET /internal/v1/resources/{id}/content", content(false))
	register("GET /v1/artifacts/{id}/content", content(true))
	register("POST /internal/v1/pins", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.AcquirePinRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.AcquirePin(r.Context(), p, in)
		respond(w, x, e)
	})
	register("GET /internal/v1/pins/{id}", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		x, e := s.GetPin(r.Context(), p, r.PathValue("id"))
		respond(w, x, e)
	})
	register("POST /internal/v1/pins/{id}/release", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		var in v.PinReleaseRequest
		if e := decode(r, &in); e != nil {
			writeError(w, e)
			return
		}
		x, e := s.ReleasePinBound(r.Context(), p, r.PathValue("id"), in)
		respond(w, x, e)
	})
	register("POST /internal/v1/pins/{id}/activate", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		x, e := s.ActivatePin(r.Context(), p, r.PathValue("id"))
		respond(w, x, e)
	})
	register("DELETE /internal/v1/pins/{id}", func(w http.ResponseWriter, r *http.Request, p v.Principal) {
		if e := s.ReleasePin(r.Context(), p, r.PathValue("id")); e != nil {
			writeError(w, e)
			return
		}
		w.WriteHeader(204)
	})
	return mux
}
