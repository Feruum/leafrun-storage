package storage

import (
	"context"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"os"
	"testing"
	"time"
)

func TestIntegrationSweepScheduleUpdateAndLegacyRetirement(t *testing.T) {
	if os.Getenv("LEAFRUN_STORAGE_TEMPORAL_INTEGRATION") != "1" {
		t.Skip("real Temporal required")
	}
	s, _ := integration(t)
	c, e := client.Dial(client.Options{HostPort: s.Config.TemporalAddress, Namespace: s.Config.TemporalNamespace})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	queue := "storage-schedule-test-" + id()
	w := worker.New(c, queue, worker.Options{MaxConcurrentActivityExecutionSize: 1})
	w.RegisterWorkflow(GCWorkflow)
	w.RegisterActivityWithOptions(s.GC, activity.RegisterOptions{Name: "StorageGC"})
	if e = w.Start(); e != nil {
		t.Fatal(e)
	}
	defer w.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	sid := "storage-schedule-test-" + id()
	legacy := sid + "-legacy"
	_, e = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: legacy, TaskQueue: queue, CronSchedule: "*/5 * * * *"}, GCWorkflow)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		handle := c.ScheduleClient().GetHandle(cleanup, sid)
		description, describeErr := handle.Describe(cleanup)
		_ = handle.Delete(cleanup)
		if describeErr == nil {
			for _, run := range description.Info.RunningWorkflows {
				_ = c.CancelWorkflow(cleanup, run.WorkflowID, "")
				_ = c.GetWorkflow(cleanup, run.WorkflowID, "").Get(cleanup, nil)
			}
		}
		_ = c.CancelWorkflow(cleanup, legacy, "")
	}()
	s.Config.TaskQueue = queue
	s.Config.SweepInterval = 7 * time.Minute
	if e = ensureSweepSchedule(ctx, c, sid, legacy, s.Config); e != nil {
		t.Fatal(e)
	}
	handle := c.ScheduleClient().GetHandle(ctx, sid)
	desc, e := handle.Describe(ctx)
	if e != nil || len(desc.Schedule.Spec.Intervals) != 1 || desc.Schedule.Spec.Intervals[0].Every != 7*time.Minute {
		t.Fatal("configured schedule was not persisted", e)
	}
	if desc.Schedule.Policy.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		t.Fatal("schedule overlap allowed")
	}
	old, e := c.DescribeWorkflowExecution(ctx, legacy, "")
	if e != nil || old.WorkflowExecutionInfo.Status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		t.Fatal("legacy recurrence remained running", e)
	}
	s.Config.SweepInterval = 11 * time.Minute
	for range 2 {
		if e = ensureSweepSchedule(ctx, c, sid, legacy, s.Config); e != nil {
			t.Fatal("restart schedule update/retry", e)
		}
	}
	updated, e := handle.Describe(ctx)
	if e != nil || updated.Schedule.Spec.Intervals[0].Every != 11*time.Minute {
		t.Fatal("restart ignored changed interval", e)
	}
	action, ok := updated.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || action.ID != sid+"-sweep" || action.TaskQueue != queue {
		t.Fatal("stable schedule/action identity changed")
	}
	if e = handle.Pause(ctx, client.SchedulePauseOptions{Note: "test administrative pause"}); e != nil {
		t.Fatal(e)
	}
	if e = ensureSweepSchedule(ctx, c, sid, legacy, s.Config); e != nil {
		t.Fatal(e)
	}
	paused, e := handle.Describe(ctx)
	if e != nil || !paused.Schedule.State.Paused {
		t.Fatal("restart lost administrative pause", e)
	}
	for len(paused.Info.RecentActions) == 0 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
		paused, e = handle.Describe(ctx)
		if e != nil {
			t.Fatal(e)
		}
	}
	if len(paused.Info.RecentActions) == 0 {
		t.Fatal("schedule did not dispatch the initial sweep")
	}
	for _, a := range paused.Info.RecentActions {
		if a.StartWorkflowResult != nil {
			if e = c.GetWorkflow(ctx, a.StartWorkflowResult.WorkflowID, "").Get(ctx, nil); e != nil {
				t.Fatal("scheduled sweep did not complete", e)
			}
		}
	}
}
