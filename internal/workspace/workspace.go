package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fluxa-dev/fluxa/internal/config"
)

type Workspace struct {
	Root     string
	Manifest config.Manifest
}

func Find(start string) (Workspace, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return Workspace{}, err
	}
	for {
		manifest := filepath.Join(dir, "Fluxa.toml")
		if _, err := os.Stat(manifest); err == nil {
			m, err := config.Load(manifest)
			return Workspace{Root: dir, Manifest: m}, err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return Workspace{}, fmt.Errorf("no Fluxa.toml found from %s", start)
		}
		dir = parent
	}
}

func Init(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	path = abs
	if err := os.MkdirAll(filepath.Join(path, "workflows"), 0755); err != nil {
		return err
	}
	manifest := `[workspace]
api_version = 1
default_timezone = "UTC"

[workflows.example]
entry = "workflows/example.lua"
timeout = "30s"
`
	script := `log.info("Starting example workflow", {
  execution_id = fluxa.execution_id,
})

local todo_response = task("fetch-todo")
  :timeout("10s")
  :run(function()
    return http.get("https://jsonplaceholder.typicode.com/todos/1")
  end)

local todo = json.decode(todo_response.body)

local user_response = task("fetch-user")
  :key(todo.userId)
  :timeout("10s")
  :run(function()
    return http.get("https://jsonplaceholder.typicode.com/users/" .. todo.userId)
  end)

local user = json.decode(user_response.body)

log.info("Example complete", { todo = todo.title, user = user.name })

return { ok = true, todo = todo.title, user = user.name }
`
	if err := writeNew(filepath.Join(path, "Fluxa.toml"), manifest); err != nil {
		return err
	}
	if err := writeNew(filepath.Join(path, "workflows", "example.lua"), script); err != nil {
		return err
	}
	if err := writeNew(filepath.Join(path, ".gitignore"), ".fluxa/\n"); err != nil {
		return err
	}
	return register(path)
}
func writeNew(path, body string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("refusing to overwrite %s", path)
	}
	return os.WriteFile(path, []byte(body), 0644)
}

// register records only the absolute workspace path. It contains no secrets and
// lets the uninstall script remove workspaces explicitly created by Fluxa.
func register(path string) error {
	root, err := os.UserConfigDir()
	if err != nil {
		return fmt.Errorf("locate Fluxa user config: %w", err)
	}
	dir := filepath.Join(root, "fluxa")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	registry := filepath.Join(dir, "workspaces")
	b, err := os.ReadFile(registry)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, existing := range strings.Split(string(b), "\n") {
		if existing == path {
			return nil
		}
	}
	f, err := os.OpenFile(registry, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintln(f, path)
	return err
}
