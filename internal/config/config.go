package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/robfig/cron/v3"
)

const APIVersion = 1

type Manifest struct {
	Workspace Workspace           `toml:"workspace"`
	Workflows map[string]Workflow `toml:"workflows"`
}

type Workspace struct {
	APIVersion         int    `toml:"api_version"`
	DefaultEnvironment string `toml:"default_environment"`
	DefaultTimezone    string `toml:"default_timezone"`
}

type Workflow struct {
	Entry          string   `toml:"entry"`
	Timeout        string   `toml:"timeout"`
	Secrets        []string `toml:"secrets"`
	OnFailure      string   `toml:"on_failure"`
	MaxConcurrency int      `toml:"max_concurrency"`
	Schedule       Schedule `toml:"schedule"`
	Webhook        Webhook  `toml:"webhook"`
	Defaults       Defaults `toml:"defaults"`
}

type Schedule struct {
	Cron     string `toml:"cron"`
	Timezone string `toml:"timezone"`
}
type Webhook struct {
	Method string `toml:"method"`
	Path   string `toml:"path"`
}
type Defaults struct {
	Retry Retry `toml:"retry"`
}
type Retry struct {
	MaxAttempts    int     `toml:"max_attempts"`
	InitialBackoff string  `toml:"initial_backoff"`
	MaxBackoff     string  `toml:"max_backoff"`
	Multiplier     float64 `toml:"multiplier"`
}

func Load(path string) (Manifest, error) {
	var m Manifest
	md, err := toml.DecodeFile(path, &m)
	if err != nil {
		return m, fmt.Errorf("parse %s: %w", path, err)
	}
	if unknown := md.Undecoded(); len(unknown) > 0 {
		return m, fmt.Errorf("%s: unknown field %q", path, unknown[0].String())
	}
	return m, Validate(m, filepath.Dir(path))
}

func Validate(m Manifest, root string) error {
	if m.Workspace.APIVersion != APIVersion {
		return fmt.Errorf("workspace.api_version must be %d", APIVersion)
	}
	if len(m.Workflows) == 0 {
		return fmt.Errorf("at least one workflow is required")
	}
	webhookRoutes := make(map[string]string)
	for name, w := range m.Workflows {
		if name == "" || w.Entry == "" {
			return fmt.Errorf("workflow %q must define entry", name)
		}
		if filepath.IsAbs(w.Entry) || strings.HasPrefix(filepath.Clean(w.Entry), ".."+string(filepath.Separator)) {
			return fmt.Errorf("workflow %q entry must remain inside workspace", name)
		}
		if _, err := os.Stat(filepath.Join(root, w.Entry)); err != nil {
			return fmt.Errorf("workflow %q entry: %w", name, err)
		}
		if w.Timeout != "" {
			if _, err := time.ParseDuration(w.Timeout); err != nil {
				return fmt.Errorf("workflow %q timeout: %w", name, err)
			}
		}
		if w.MaxConcurrency < 0 {
			return fmt.Errorf("workflow %q max_concurrency must be non-negative", name)
		}
		if w.OnFailure != "" {
			if _, ok := m.Workflows[w.OnFailure]; !ok {
				return fmt.Errorf("workflow %q references unknown on_failure workflow %q", name, w.OnFailure)
			}
		}
		if w.Schedule.Timezone != "" {
			if _, err := time.LoadLocation(w.Schedule.Timezone); err != nil {
				return fmt.Errorf("workflow %q schedule timezone: %w", name, err)
			}
		}
		if w.Schedule.Cron != "" {
			if _, err := cron.ParseStandard(w.Schedule.Cron); err != nil {
				return fmt.Errorf("workflow %q schedule cron: %w", name, err)
			}
		}
		if w.Webhook.Path != "" {
			if !strings.HasPrefix(w.Webhook.Path, "/") {
				return fmt.Errorf("workflow %q webhook path must start with /", name)
			}
			if w.Webhook.Method == "" {
				return fmt.Errorf("workflow %q webhook method is required", name)
			}
			route := strings.ToUpper(w.Webhook.Method) + " " + w.Webhook.Path
			if other, exists := webhookRoutes[route]; exists {
				return fmt.Errorf("workflows %q and %q declare the same webhook %s", other, name, route)
			}
			webhookRoutes[route] = name
		}
	}
	return nil
}
