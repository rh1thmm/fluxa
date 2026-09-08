package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/store"
)

func TestWaitForPersistsAndResumeReplays(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "workflow.lua"), []byte(`
local value = task("before-wait"):run(function() return { n = 1 } end)
wait.sleep("5ms")
return { n = value.n + 1 }
`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(root, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	w := config.Workflow{Entry: "workflow.lua"}
	artifact, err := ArtifactFor(root, "example", w)
	if err != nil {
		t.Fatal(err)
	}
	r := New(s)
	executionID, err := r.Run(context.Background(), root, "example", w, artifact, map[string]any{"trigger": "manual"})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := s.Execution(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != store.ExecutionWaiting {
		t.Fatalf("status = %s, want waiting", execution.Status)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := r.Resume(context.Background(), root, config.Manifest{Workflows: map[string]config.Workflow{"example": w}}, executionID); err != nil {
		t.Fatal(err)
	}
	execution, err = s.Execution(context.Background(), executionID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Status != store.ExecutionCompleted {
		t.Fatalf("status = %s, want completed", execution.Status)
	}
}
