package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRejectsEscapingEntry(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Fluxa.toml"), []byte("[workspace]\napi_version=1\n[workflows.bad]\nentry='../outside.lua'\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(dir, "Fluxa.toml")); err == nil {
		t.Fatal("expected escaping entry error")
	}
}

func TestLoadAcceptsWorkflow(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "workflows"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflows", "ok.lua"), []byte("return function() end"), 0644); err != nil {
		t.Fatal(err)
	}
	manifest := "[workspace]\napi_version=1\n[workflows.ok]\nentry='workflows/ok.lua'\ntimeout='1s'\n"
	if err := os.WriteFile(filepath.Join(dir, "Fluxa.toml"), []byte(manifest), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(dir, "Fluxa.toml")); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "workflows"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "workflows", "ok.lua"), []byte("return function() end"), 0644); err != nil {
		t.Fatal(err)
	}
	manifest := "[workspace]\napi_version=1\n[workflows.ok]\nentry='workflows/ok.lua'\non_failuer='typo'\n"
	if err := os.WriteFile(filepath.Join(dir, "Fluxa.toml"), []byte(manifest), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(filepath.Join(dir, "Fluxa.toml")); err == nil {
		t.Fatal("expected unknown-field error")
	}
}
