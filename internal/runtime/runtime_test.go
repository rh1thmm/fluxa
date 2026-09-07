package runtime

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/store"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRunPersistsTaskAndHTTPOperation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(`local result = task("fetch")
  :run(function()
    local r = http.get("`+server.URL+`")
    return json.decode(r.body)
  end)
return result`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	artifact, err := ArtifactFor(dir, "test", w)
	if err != nil {
		t.Fatal(err)
	}
	r := New(s)
	id, err := r.Run(context.Background(), dir, "test", w, artifact, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Execution(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != "completed" {
		t.Fatalf("status = %s", e.Status)
	}
	events, err := s.Timeline(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(events, "\n"), "operation.completed") {
		t.Fatalf("expected HTTP operation in %#v", events)
	}
}

func TestRetryReplaysCompletedOperationAndRetriesLaterSafeFailure(t *testing.T) {
	var stepACalls, stepBCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/step-a":
			stepACalls.Add(1)
			_, _ = w.Write([]byte(`{"value":1}`))
		case "/step-b":
			stepBCalls.Add(1)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	source := `local result = task("example")
  :key("main")
  :run(function()
    local first = http.post("` + server.URL + `/step-a", { json = { value = 1 } })
    local transformed = json.decode(first.body)
    transformed.value = transformed.value + 1
    return http.get("` + server.URL + `/step-b")
  end)
return result`
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	artifact, err := ArtifactFor(dir, "test", w)
	if err != nil {
		t.Fatal(err)
	}
	r := New(s)
	transport := http.DefaultTransport
	var failedB atomic.Bool
	r.HTTP = &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/step-b" && !failedB.Swap(true) {
			return nil, fmt.Errorf("synthetic pre-dispatch failure")
		}
		return transport.RoundTrip(req)
	})}
	id, err := r.Run(context.Background(), dir, "test", w, artifact, nil)
	if err == nil {
		t.Fatal("first run unexpectedly succeeded")
	}
	e, err := s.Execution(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != store.ExecutionFailed {
		t.Fatalf("first status = %s", e.Status)
	}
	if got := stepACalls.Load(); got != 1 {
		t.Fatalf("step A calls after failure = %d, want 1", got)
	}
	manifest := config.Manifest{Workflows: map[string]config.Workflow{"test": w}}
	if _, err := r.Retry(context.Background(), dir, manifest, id, false); err != nil {
		t.Fatalf("retry: %v", err)
	}
	e, err = s.Execution(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != store.ExecutionCompleted {
		t.Fatalf("retry status = %s: %s", e.Status, e.Error)
	}
	if got := stepACalls.Load(); got != 1 {
		t.Fatalf("step A was replayed over network %d times", got)
	}
	if got := stepBCalls.Load(); got != 1 {
		t.Fatalf("step B successful server calls = %d, want 1", got)
	}
}

func TestAmbiguousPOSTRequiresForce(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			hj := w.(http.Hijacker)
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Fatal(err)
			}
			_ = conn.Close()
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(`return task("send")
  :key("main")
  :run(function()
    return http.post("`+server.URL+`", { json = { value = 1 }, idempotency = true })
  end)`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	a, err := ArtifactFor(dir, "test", w)
	if err != nil {
		t.Fatal(err)
	}
	r := New(s)
	id, err := r.Run(context.Background(), dir, "test", w, a, nil)
	if err == nil {
		t.Fatal("expected ambiguous failure")
	}
	if _, err := r.Retry(context.Background(), dir, config.Manifest{Workflows: map[string]config.Workflow{"test": w}}, id, false); err == nil {
		t.Fatal("retry without force succeeded")
	}
	if _, err := r.Retry(context.Background(), dir, config.Manifest{Workflows: map[string]config.Workflow{"test": w}}, id, true); err != nil {
		t.Fatalf("forced retry: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d want 2", calls.Load())
	}
}

func TestRetryRejectsChangedWorkflowArtifact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.lua")
	if err := os.WriteFile(path, []byte(`return task("fail"):key("main"):run(function() error("boom") end)`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	a, err := ArtifactFor(dir, "test", w)
	if err != nil {
		t.Fatal(err)
	}
	r := New(s)
	id, err := r.Run(context.Background(), dir, "test", w, a, nil)
	if err == nil {
		t.Fatal("expected failure")
	}
	if err := os.WriteFile(path, []byte(`-- changed\nreturn task("fail"):key("main"):run(function() error("boom") end)`), 0644); err != nil {
		t.Fatal(err)
	}
	_, err = r.Retry(context.Background(), dir, config.Manifest{Workflows: map[string]config.Workflow{"test": w}}, id, false)
	if err == nil || !strings.Contains(err.Error(), "workflow_version_mismatch") {
		t.Fatalf("retry error = %v", err)
	}
}

func TestDuplicateTaskKeyFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(`task("item"):key("same"):run(function() return 1 end)
task("item"):key("same"):run(function() return 2 end)`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	a, err := ArtifactFor(dir, "test", w)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(s).Run(context.Background(), dir, "test", w, a, nil)
	if err == nil || !strings.Contains(err.Error(), "task_identity_conflict") {
		t.Fatalf("run error = %v", err)
	}
}

func TestFluentBuilderRetriesAndRejectsDoubleRun(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(`local attempts = 0
local value = task("retrying"):retry(1):timeout("1s"):run(function()
  attempts = attempts + 1
  if attempts == 1 then error("retry me") end
  return attempts
end)
local b = task("once")
b:run(function() return value end)
b:run(function() return value end)`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	a, err := ArtifactFor(dir, "test", w)
	if err != nil {
		t.Fatal(err)
	}
	id, err := New(s).Run(context.Background(), dir, "test", w, a, nil)
	if err == nil || !strings.Contains(err.Error(), "already executed") {
		t.Fatalf("run error = %v", err)
	}
	tasks, err := s.Tasks(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 || tasks[0].Attempt != 2 || tasks[0].Status != store.ExecutionCompleted {
		t.Fatalf("tasks = %#v", tasks)
	}
}

func TestTopLevelWorkflowCanUseFluxaContext(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(`return { workflow = fluxa.workflow, execution = fluxa.execution_id }`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	a, err := ArtifactFor(dir, "example", w)
	if err != nil {
		t.Fatal(err)
	}
	id, err := New(s).Run(context.Background(), dir, "example", w, a, nil)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.Execution(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(e.Result), `"workflow":"example"`) {
		t.Fatalf("result = %s", e.Result)
	}
}
