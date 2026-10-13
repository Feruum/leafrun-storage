package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"os"
	"testing"
	"time"
)

func TestIntegrationTemporalGCAndReplay(t *testing.T) {
	if os.Getenv("LEAFRUN_STORAGE_TEMPORAL_INTEGRATION") != "1" {
		t.Skip("set LEAFRUN_STORAGE_TEMPORAL_INTEGRATION=1 with real Temporal")
	}
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	r := committed(t, s, p, scope, "temporal garbage")
	_, e := s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '2 days' WHERE id=$1", r.ResourceID)
	if e != nil {
		t.Fatal(e)
	}
	c, e := client.Dial(client.Options{HostPort: s.Config.TemporalAddress, Namespace: s.Config.TemporalNamespace})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	queue := "storage-test-" + id()
	w := worker.New(c, queue, worker.Options{MaxConcurrentActivityExecutionSize: 1})
	w.RegisterWorkflow(GCWorkflow)
	w.RegisterActivityWithOptions(s.GC, activity.RegisterOptions{Name: "StorageGC"})
	if e = w.Start(); e != nil {
		t.Fatal(e)
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	run, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "storage-test-gc-" + id(), TaskQueue: queue}, GCWorkflow)
	if e != nil {
		t.Fatal(e)
	}
	if e = run.Get(ctx, nil); e != nil {
		t.Fatal(e)
	}
	o, e := getObject(ctx, s.DB.Pool, r.ResourceID)
	if e != nil || o.State != "deleted" {
		t.Fatal("real workflow did not collect", e)
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(GCWorkflow)
	if e = replayer.ReplayWorkflowExecution(ctx, c.WorkflowService(), nil, s.Config.TemporalNamespace, workflow.Execution{ID: run.GetID(), RunID: run.GetRunID()}); e != nil {
		t.Fatal("workflow replay failed", e)
	}
}
