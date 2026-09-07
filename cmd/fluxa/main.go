package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fluxa-dev/fluxa/internal/runtime"
	"github.com/fluxa-dev/fluxa/internal/store"
	"github.com/fluxa-dev/fluxa/internal/workspace"
)

var version = "dev"
var commit = "none"
var buildDate = "unknown"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fluxa:", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		usage()
		return nil
	}
	if args[0] == "--version" || args[0] == "version" {
		fmt.Printf("fluxa %s (%s, %s)\n", version, commit, buildDate)
		return nil
	}
	switch args[0] {
	case "init":
		if len(args) != 2 {
			return fmt.Errorf("usage: fluxa init <directory>")
		}
		if err := workspace.Init(args[1]); err != nil {
			return err
		}
		fmt.Println("Initialized Fluxa workspace:", args[1])
		return nil
	case "validate":
		return validate(args[1:])
	case "workflows":
		return workflows()
	case "run":
		return runWorkflow(args[1:])
	case "retry":
		return retryWorkflow(args[1:])
	case "runs":
		return runs(args[1:])
	case "inspect":
		return inspect(args[1:])
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}
func ws() (workspace.Workspace, error) { return workspace.Find(".") }
func validate(args []string) error {
	w, err := ws()
	if err != nil {
		return err
	}
	if len(args) > 1 {
		return fmt.Errorf("usage: fluxa validate [workflow]")
	}
	if len(args) == 1 {
		if _, ok := w.Manifest.Workflows[args[0]]; !ok {
			return fmt.Errorf("unknown workflow %q", args[0])
		}
	}
	for name, workflow := range w.Manifest.Workflows {
		if len(args) == 1 && name != args[0] {
			continue
		}
		if err := runtime.Compile(filepath.Join(w.Root, workflow.Entry)); err != nil {
			return fmt.Errorf("workflow %q Lua syntax: %w", name, err)
		}
	}
	fmt.Printf("valid: %s (%d workflow(s))\n", w.Root, len(w.Manifest.Workflows))
	return nil
}
func workflows() error {
	w, err := ws()
	if err != nil {
		return err
	}
	for n, v := range w.Manifest.Workflows {
		fmt.Printf("%-24s %s\n", n, v.Entry)
	}
	return nil
}
func openStore(w workspace.Workspace) (*store.Store, error) {
	return store.Open(filepath.Join(w.Root, ".fluxa", "state.db"))
}
func runWorkflow(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: fluxa run <workflow>")
	}
	w, err := ws()
	if err != nil {
		return err
	}
	wf, ok := w.Manifest.Workflows[args[0]]
	if !ok {
		return fmt.Errorf("unknown workflow %q", args[0])
	}
	s, err := openStore(w)
	if err != nil {
		return err
	}
	defer s.Close()
	artifact, err := runtime.ArtifactFor(w.Root, args[0], wf)
	if err != nil {
		return err
	}
	r := runtime.New(s)
	eid, err := r.Run(context.Background(), w.Root, args[0], wf, artifact, map[string]any{})
	fmt.Println("execution:", eid)
	return err
}
func retryWorkflow(args []string) error {
	force := false
	if len(args) == 2 && args[1] == "--force" {
		force = true
		args = args[:1]
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: fluxa retry <execution-id> [--force]")
	}
	w, err := ws()
	if err != nil {
		return err
	}
	s, err := openStore(w)
	if err != nil {
		return err
	}
	defer s.Close()
	eid, err := runtime.New(s).Retry(context.Background(), w.Root, w.Manifest, args[0], force)
	fmt.Println("execution:", eid)
	return err
}
func runs(args []string) error {
	if len(args) > 1 {
		return fmt.Errorf("usage: fluxa runs [workflow]")
	}
	w, err := ws()
	if err != nil {
		return err
	}
	s, err := openStore(w)
	if err != nil {
		return err
	}
	defer s.Close()
	name := ""
	if len(args) == 1 {
		name = args[0]
	}
	all, err := s.Runs(context.Background(), name)
	if err != nil {
		return err
	}
	for _, e := range all {
		fmt.Printf("%s  %-18s %-18s %s\n", e.ID, e.Status, e.Workflow, e.StartedAt.Format("2006-01-02 15:04:05Z"))
	}
	return nil
}
func inspect(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: fluxa inspect <execution-id>")
	}
	w, err := ws()
	if err != nil {
		return err
	}
	s, err := openStore(w)
	if err != nil {
		return err
	}
	defer s.Close()
	e, err := s.Execution(context.Background(), args[0])
	if err != nil {
		return err
	}
	fmt.Printf("%s %s %s\n", e.ID, e.Workflow, e.Status)
	if e.Error != "" {
		fmt.Println("error:", e.Error)
	}
	events, err := s.Timeline(context.Background(), e.ID)
	if err != nil {
		return err
	}
	fmt.Println(strings.Join(events, "\n"))
	return nil
}
func usage() {
	fmt.Fprintln(os.Stderr, `Fluxa — durable Lua automation

Usage: fluxa <command>
  init <directory>       create a workspace
  validate [workflow]    validate manifest and workflow selection
  workflows              list workflows
  run <workflow>         execute a workflow
	  retry <execution-id> [--force] resume a failed execution
  runs [workflow]        list executions
  inspect <execution-id> show durable execution events`)
}
