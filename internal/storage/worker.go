package storage

import (
	"context"
	"errors"
	"fmt"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"time"
)

const GCWorkflowID = "leafrun-storage-gc"

func GCWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 4 * time.Minute, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 10 * time.Second, MaximumInterval: time.Minute, MaximumAttempts: 5}})
	return workflow.ExecuteActivity(ctx, "StorageGC").Get(ctx, nil)
}

// retireLegacyCron confirms the old recurring workflow has closed before adding a
// replacement recurrence. The catalog's GC session lock also fences in-flight activities.
func retireLegacyCron(ctx context.Context, c client.Client, legacyID string) error {
	if legacyID == "" {
		return nil
	}
	d, e := c.DescribeWorkflowExecution(ctx, legacyID, "")
	var missing *serviceerror.NotFound
	if errors.As(e, &missing) {
		return nil
	}
	if e != nil {
		return e
	}
	if d.WorkflowExecutionInfo.Status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}
	runID := d.WorkflowExecutionInfo.Execution.RunId
	started, e := c.GetWorkflowHistory(ctx, legacyID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT).Next()
	if e != nil {
		return e
	}
	if started.GetWorkflowExecutionStartedEventAttributes().GetCronSchedule() == "" {
		return nil
	}
	// Address the latest run: a cron continuation may have happened after Describe.
	if e = c.CancelWorkflow(ctx, legacyID, ""); e != nil && !errors.As(e, &missing) {
		return e
	}
	_ = c.GetWorkflow(ctx, legacyID, "").Get(ctx, nil)
	if e = ctx.Err(); e != nil {
		return e
	}
	d, e = c.DescribeWorkflowExecution(ctx, legacyID, "")
	if e != nil {
		return e
	}
	if d.WorkflowExecutionInfo.Status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return fmt.Errorf("legacy Storage GC recurrence is still running")
	}
	return nil
}

// One namespace-scoped schedule is updated in place on restart. Interval specs
// retain duration semantics without cron's uneven minute/hour boundaries.
func ensureSweepSchedule(ctx context.Context, c client.Client, scheduleID, legacyID string, cfg Config) error {
	if cfg.SweepInterval < time.Second || cfg.SweepInterval > 24*time.Hour || cfg.SweepInterval%time.Second != 0 {
		return fmt.Errorf("invalid Storage sweep interval")
	}
	if e := retireLegacyCron(ctx, c, legacyID); e != nil {
		return e
	}
	spec := client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: cfg.SweepInterval}}}
	action := &client.ScheduleWorkflowAction{ID: scheduleID + "-sweep", Workflow: GCWorkflow, TaskQueue: cfg.TaskQueue}
	_, e := c.ScheduleClient().Create(ctx, client.ScheduleOptions{ID: scheduleID, Spec: spec, Action: action, Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP, TriggerImmediately: true})
	if e == nil {
		return nil
	}
	if !errors.Is(e, temporal.ErrScheduleAlreadyRunning) {
		return e
	}
	return c.ScheduleClient().GetHandle(ctx, scheduleID).Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
		schedule := in.Description.Schedule
		schedule.Spec = &spec
		schedule.Action = action
		if schedule.Policy == nil {
			schedule.Policy = &client.SchedulePolicies{}
		}
		schedule.Policy.Overlap = enums.SCHEDULE_OVERLAP_POLICY_SKIP
		return &client.ScheduleUpdate{Schedule: &schedule}, nil
	}})
}

func (s *Service) RunWorker(ctx context.Context) error {
	c, e := client.Dial(client.Options{HostPort: s.Config.TemporalAddress, Namespace: s.Config.TemporalNamespace})
	if e != nil {
		return e
	}
	defer c.Close()
	w := worker.New(c, s.Config.TaskQueue, worker.Options{MaxConcurrentActivityExecutionSize: 1})
	w.RegisterWorkflow(GCWorkflow)
	w.RegisterActivityWithOptions(s.GC, activity.RegisterOptions{Name: "StorageGC"})
	if e = w.Start(); e != nil {
		return e
	}
	defer w.Stop()
	startup, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if e = ensureSweepSchedule(startup, c, GCWorkflowID, GCWorkflowID, s.Config); e != nil {
		return e
	}
	<-ctx.Done()
	return nil
}
