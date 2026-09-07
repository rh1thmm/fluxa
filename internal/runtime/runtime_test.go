package runtime

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/store"
)

func TestRunPersistsTaskAndHTTPOperation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "workflow.lua"), []byte(`return function(ctx)
  return task("fetch", function()
    local r = http.get("`+server.URL+`")
    return json.decode(r.body)
  end)
end`), 0644); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dir, ".fluxa", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := New(s)
	id, err := r.Run(context.Background(), dir, "test", config.Workflow{Entry: "workflow.lua"}, Version(filepath.Join(dir, "workflow.lua")), nil)
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
