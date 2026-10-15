package storage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	v "github.com/leafrun/leafrun-contracts/storage/v1"
)

type resourceMetadataOwner struct {
	*testOwner
	scope  v.Scope
	checks []v.AccessCheck
}

func (o *resourceMetadataOwner) Access(_ context.Context, check v.AccessCheck) (bool, error) {
	o.checks = append(o.checks, check)
	return check.Scope == o.scope && !o.deny, o.err
}

func TestIntegrationResourceMetadataRequiresCurrentAccessAndReadyCommittedResource(t *testing.T) {
	s, p := integration(t)
	ctx := context.Background()
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	otherScope := v.Scope{WorkspaceID: scope.WorkspaceID, ProjectID: id()}
	resource := committed(t, s, p, scope, "metadata bytes")
	other := committed(t, s, p, otherScope, "private bytes")
	uncommitted := uploaded(t, s, p, scope, v.ClassResource, "uncommitted")
	result := uploaded(t, s, p, scope, v.ClassResult, "%PDF-result")
	recoveryUpload := uploaded(t, s, p, scope, v.ClassRecovery, "recovery bytes")
	recovery, err := s.Commit(ctx, p, recoveryUpload.UploadID, v.CommitResourceRequest{WriterToken: recoveryUpload.WriterToken, IdempotencyKey: id()})
	if err != nil {
		t.Fatal(err)
	}
	owner := &resourceMetadataOwner{testOwner: &testOwner{publish: true, state: v.OwnerReferenced}, scope: scope}
	s.Owner = owner
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	get := func(objectID, token string, want int) *http.Response {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, server.URL+"/internal/v1/resources/"+objectID, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+token)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			response.Body.Close()
			t.Fatalf("metadata status for %q: got %d want %d", objectID, response.StatusCode, want)
		}
		return response
	}
	token := s.Config.Credentials[0].Token
	response := get(resource.ResourceID, token, http.StatusOK)
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("private metadata can be cached")
	}
	var metadata v.Resource
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&metadata)
	response.Body.Close()
	if err != nil || metadata.ResourceID != resource.ResourceID || metadata.Scope != scope || metadata.Class != v.ClassResource || metadata.SHA256 != resource.SHA256 || metadata.SizeBytes != resource.SizeBytes || !metadata.CreatedAt.Equal(resource.CreatedAt) {
		t.Fatalf("metadata differs from committed resource: %+v, %v", metadata, err)
	}
	if len(owner.checks) != 1 || owner.checks[0].Operation != "resource.read" || owner.checks[0].ObjectID != resource.ResourceID || owner.checks[0].Scope != scope {
		t.Fatalf("metadata skipped authoritative access: %+v", owner.checks)
	}
	response = get(recovery.ResourceID, token, http.StatusOK)
	decoder = json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&metadata)
	response.Body.Close()
	if err != nil || metadata.Class != v.ClassRecovery || metadata.SHA256 != recovery.SHA256 || metadata.Scope != scope {
		t.Fatalf("recovery metadata: %+v, %v", metadata, err)
	}
	if _, err = s.DB.Pool.Exec(ctx, "UPDATE storage_objects SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", recovery.ResourceID); err != nil {
		t.Fatal(err)
	}
	get(recovery.ResourceID, token, http.StatusGone).Body.Close()
	for _, objectID := range []string{other.ResourceID, id(), uncommitted.UploadID, result.UploadID} {
		get(objectID, token, http.StatusNotFound).Body.Close()
	}
	get(resource.ResourceID, "invalid-token", http.StatusUnauthorized).Body.Close()
	owner.deny = true
	get(resource.ResourceID, token, http.StatusNotFound).Body.Close()
	owner.deny = false
	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unavailable.Close()
	s.Owner = NewOwner(unavailable.URL, "fixed-owner-token")
	get(resource.ResourceID, token, http.StatusServiceUnavailable).Body.Close()
	s.Owner = owner
	if _, err = s.DB.Pool.Exec(ctx, "UPDATE storage_objects SET state='deleting' WHERE id=$1", resource.ResourceID); err != nil {
		t.Fatal(err)
	}
	get(resource.ResourceID, token, http.StatusConflict).Body.Close()
	if _, err = s.DB.Pool.Exec(ctx, "UPDATE storage_objects SET state='ready',expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", resource.ResourceID); err != nil {
		t.Fatal(err)
	}
	get(resource.ResourceID, token, http.StatusGone).Body.Close()
	owner.deny = true
	get(resource.ResourceID, token, http.StatusNotFound).Body.Close()
}
