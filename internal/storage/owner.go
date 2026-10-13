package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"net/http"
	"strings"
	"time"
)

type Owner interface {
	Access(context.Context, v.AccessCheck) (bool, error)
	Publishable(context.Context, v.PublishabilityCheck) (bool, error)
	Pin(context.Context, v.PinOwnerCheck) (v.OwnerState, error)
}
type HTTPOwner struct {
	URL, Token string
	Client     *http.Client
}

func NewOwner(url, token string) *HTTPOwner {
	return &HTTPOwner{strings.TrimRight(url, "/"), token, &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func (o *HTTPOwner) call(ctx context.Context, path string, input, output any) error {
	b, e := json.Marshal(input)
	if e != nil {
		return e
	}
	r, e := http.NewRequestWithContext(ctx, "POST", o.URL+"/internal/v1/checks/"+path, bytes.NewReader(b))
	if e != nil {
		return e
	}
	r.Header.Set("Authorization", "Bearer "+o.Token)
	r.Header.Set("Content-Type", "application/json")
	res, e := o.Client.Do(r)
	if e != nil {
		return fail(503, "owner_unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fail(503, "owner_unavailable")
	}
	if e = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(output); e != nil {
		return fail(503, "owner_invalid_response")
	}
	return nil
}
func (o *HTTPOwner) Access(c context.Context, x v.AccessCheck) (bool, error) {
	var d v.AccessDecision
	e := o.call(c, "access", x, &d)
	return d.Allowed, e
}
func (o *HTTPOwner) Publishable(c context.Context, x v.PublishabilityCheck) (bool, error) {
	var d v.PublishabilityDecision
	e := o.call(c, "publishable", x, &d)
	return d.Publishable, e
}
func (o *HTTPOwner) Pin(c context.Context, x v.PinOwnerCheck) (v.OwnerState, error) {
	var d v.PinOwnerDecision
	e := o.call(c, "pin-owner", x, &d)
	if e == nil && d.State != v.OwnerReferenced && d.State != v.OwnerAbsent && d.State != v.OwnerUnknown {
		e = fmt.Errorf("invalid owner state")
	}
	return d.State, e
}
