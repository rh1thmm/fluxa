// Package daemon owns Fluxa's long-running local trigger lifecycle.
package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/runtime"
	"github.com/fluxa-dev/fluxa/internal/store"
	"github.com/robfig/cron/v3"
)

const maxWebhookBody = 4 << 20

// Service runs all local trigger sources. Each source starts the same durable
// workflow runner and supplies a structured Fluxa input value.
type Service struct {
	Root     string
	Manifest config.Manifest
	Store    *store.Store
	Runner   *runtime.Runner
}

func New(root string, manifest config.Manifest, s *store.Store) *Service {
	return &Service{Root: root, Manifest: manifest, Store: s, Runner: runtime.New(s)}
}

// Run schedules due workflows until ctx is cancelled. It deliberately remains
// foreground; systemd/launchd can supervise this exact process.
func (s *Service) Run(ctx context.Context) error {
	if err := s.Store.RecoverQueue(ctx); err != nil {
		return err
	}
	if err := s.Tick(ctx, time.Now()); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if err := s.Tick(ctx, now); err != nil {
				return err
			}
		}
	}
}

// Serve runs the scheduler and inbound webhook listener under one lifecycle.
func (s *Service) Serve(ctx context.Context, addr string) error {
	server := &http.Server{Addr: addr, Handler: s.WebhookHandler()}
	errCh := make(chan error, 2)
	go func() {
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	go func() { errCh <- s.Run(ctx) }()
	return <-errCh
}

// Tick is exported for deterministic tests and future service supervision.
func (s *Service) Tick(ctx context.Context, now time.Time) error {
	if err := s.resumeDueWaits(ctx, now); err != nil {
		return err
	}
	for name, workflow := range s.Manifest.Workflows {
		if workflow.Schedule.Cron == "" {
			continue
		}
		active, err := s.Store.WorkflowActive(ctx, name)
		if err != nil {
			return err
		}
		if !active {
			continue
		}
		location := time.UTC
		if workflow.Schedule.Timezone != "" {
			location, err = time.LoadLocation(workflow.Schedule.Timezone)
			if err != nil {
				return fmt.Errorf("workflow %s timezone: %w", name, err)
			}
		}
		schedule, err := cron.ParseStandard(workflow.Schedule.Cron)
		if err != nil {
			return fmt.Errorf("workflow %s schedule: %w", name, err)
		}
		next, err := s.Store.ScheduleNextRun(ctx, name)
		if err != nil {
			return err
		}
		if next == nil {
			if err := s.Store.SetScheduleNextRun(ctx, name, schedule.Next(now.In(location))); err != nil {
				return err
			}
			continue
		}
		if now.Before(*next) {
			continue
		}

		// A due run waits in schedule_state while the workflow is full. Advancing
		// the cursor before this check would silently discard work.
		if workflow.MaxConcurrency > 0 {
			running, err := s.Store.RunningWorkflowCount(ctx, name)
			if err != nil {
				return err
			}
			if running >= workflow.MaxConcurrency {
				continue
			}
		}

		scheduled := *next
		claimed, err := s.Store.ClaimScheduleRun(ctx, name, scheduled)
		if err != nil {
			return err
		}
		if !claimed {
			if err := s.Store.SetScheduleNextRun(ctx, name, schedule.Next(now.In(location))); err != nil {
				return err
			}
			continue
		}
		if err := s.Store.SetScheduleNextRun(ctx, name, schedule.Next(now.In(location))); err != nil {
			return err
		}
		if _, err := s.enqueue(ctx, name, workflow, map[string]any{
			"trigger":      "schedule",
			"scheduled_at": scheduled.UTC().Format(time.RFC3339Nano),
		}); err != nil {
			return err
		}
	}
	return s.dispatch(ctx)
}

func (s *Service) resumeDueWaits(ctx context.Context, now time.Time) error {
	ids, err := s.Store.DueWaitingExecutions(ctx, now)
	if err != nil {
		return err
	}
	for _, executionID := range ids {
		go func(id string) { _, _ = s.Runner.Resume(context.Background(), s.Root, s.Manifest, id) }(executionID)
	}
	return nil
}

func (s *Service) enqueue(ctx context.Context, name string, workflow config.Workflow, input any) (string, error) {
	artifact, err := runtime.ArtifactFor(s.Root, name, workflow)
	if err != nil {
		return "", err
	}
	executionID := runtime.NewExecutionID()
	err = s.Store.EnqueueExecution(ctx, store.Execution{ID: executionID, Workflow: name, Version: artifact.Version}, input, artifact)
	return executionID, err
}

func (s *Service) dispatch(ctx context.Context) error {
	for {
		item, err := s.Store.ClaimQueuedExecution(ctx)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		workflow, ok := s.Manifest.Workflows[item.Workflow]
		if !ok {
			_ = s.Store.FinishExecution(context.Background(), item.ExecutionID, store.ExecutionFailed, "workflow no longer exists")
			_ = s.Store.FinishQueuedExecution(context.Background(), item.ExecutionID)
			continue
		}
		if workflow.MaxConcurrency > 0 {
			running, countErr := s.Store.RunningWorkflowCount(ctx, item.Workflow)
			if countErr != nil {
				return countErr
			}
			if running > workflow.MaxConcurrency {
				return s.Store.ReleaseQueuedExecution(ctx, item.ExecutionID)
			}
		}
		go s.runQueued(item, workflow)
	}
}
func (s *Service) runQueued(item store.QueuedExecution, workflow config.Workflow) {
	artifact, err := runtime.ArtifactFor(s.Root, item.Workflow, workflow)
	if err == nil {
		err = s.Runner.RunQueued(context.Background(), s.Root, item.Workflow, workflow, artifact, item.ExecutionID, item.Attempt)
	}
	if err != nil {
		_ = s.Store.FinishExecution(context.Background(), item.ExecutionID, store.ExecutionFailed, err.Error())
	}
	_ = s.Store.FinishQueuedExecution(context.Background(), item.ExecutionID)
}

func (s *Service) WebhookHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		workflowName, workflow := s.webhookWorkflow(r.Method, r.URL.Path)
		if workflowName == "" {
			http.NotFound(w, r)
			return
		}
		active, err := s.Store.WorkflowActive(r.Context(), workflowName)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !active {
			http.Error(w, "workflow is inactive", http.StatusServiceUnavailable)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody+1))
		if err != nil || len(body) > maxWebhookBody {
			http.Error(w, "webhook request body is too large", http.StatusRequestEntityTooLarge)
			return
		}
		input := map[string]any{
			"trigger": "webhook",
			"method":  r.Method,
			"headers": r.Header,
			"query":   r.URL.Query(),
			"body":    string(body),
		}
		if strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
			var value any
			if json.Unmarshal(body, &value) == nil {
				input["json"] = value
			}
		}
		executionID, err := s.enqueue(r.Context(), workflowName, workflow, input)
		if err != nil {
			http.Error(w, fmt.Sprintf("queue webhook: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"execution_id": executionID, "status": "queued"})
	})
}

func (s *Service) webhookWorkflow(method, path string) (string, config.Workflow) {
	for name, workflow := range s.Manifest.Workflows {
		if workflow.Webhook.Path == path && strings.EqualFold(workflow.Webhook.Method, method) {
			return name, workflow
		}
	}
	return "", config.Workflow{}
}
