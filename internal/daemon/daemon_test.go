package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/store"
)

func TestWebhookStartsDurableExecutionWithInput(t *testing.T) {
	root := t.TempDir()
	writeWorkflow(t, root, "hook.lua", `return { value = fluxa.input.json.value, trigger = fluxa.input.trigger }`)
	s, err := store.Open(filepath.Join(root, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	service := New(root, config.Manifest{Workflows: map[string]config.Workflow{
		"hook": {Entry: "hook.lua", Webhook: config.Webhook{Method: "POST", Path: "/hook"}},
	}}, s)
	req := httptest.NewRequest(http.MethodPost, "/hook?source=test", strings.NewReader(`{"value":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	service.WebhookHandler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		ExecutionID string `json:"execution_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var execution store.Execution
	for {
		execution, err = s.Execution(context.Background(), response.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if execution.Status == store.ExecutionCompleted || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if execution.Status != store.ExecutionCompleted {
		t.Fatalf("status = %s", execution.Status)
	}
	var result map[string]any
	if err := json.Unmarshal(execution.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result["value"] != "hello" || result["trigger"] != "webhook" {
		t.Fatalf("result = %#v", result)
	}
}

func TestScheduleSlotRunsOnce(t *testing.T) {
	root := t.TempDir()
	writeWorkflow(t, root, "scheduled.lua", `return { ok = true }`)
	s, err := store.Open(filepath.Join(root, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	service := New(root, config.Manifest{Workflows: map[string]config.Workflow{
		"scheduled": {Entry: "scheduled.lua", Schedule: config.Schedule{Cron: "* * * * *"}},
	}}, s)
	due := time.Now().UTC().Add(-time.Minute).Truncate(time.Minute)
	if err := s.SetScheduleNextRun(context.Background(), "scheduled", due); err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		runs, err := s.Runs(context.Background(), "scheduled")
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduled workflow did not run")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := service.Tick(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	runs, err := s.Runs(context.Background(), "scheduled")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want 1", len(runs))
	}
}

func TestInactiveWebhookIsRejected(t *testing.T) {
	root := t.TempDir()
	writeWorkflow(t, root, "hook.lua", `return { ok = true }`)
	s, err := store.Open(filepath.Join(root, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetWorkflowActive(context.Background(), "hook", false); err != nil {
		t.Fatal(err)
	}
	service := New(root, config.Manifest{Workflows: map[string]config.Workflow{
		"hook": {Entry: "hook.lua", Webhook: config.Webhook{Method: "POST", Path: "/hook"}},
	}}, s)
	recorder := httptest.NewRecorder()
	service.WebhookHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/hook", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func writeWorkflow(t *testing.T, root, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}
