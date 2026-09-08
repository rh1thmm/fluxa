package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fluxa-dev/fluxa/internal/daemon"
	fluxadocs "github.com/fluxa-dev/fluxa/internal/docs"
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
		fmt.Printf("Created Fluxa workspace: %s\n\n  Fluxa.toml\n  workflows/example.lua\n  .gitignore\n\nNext:\n\n  cd %s\n  fluxa run example\n", args[1], args[1])
		return nil
	case "validate":
		return validate(args[1:])
	case "workflows":
		return workflows()
	case "run":
		return runWorkflow(args[1:])
	case "daemon":
		return runDaemon(args[1:])
	case "activate", "deactivate":
		return setActivation(args[0], args[1:])
	case "schedules":
		return schedules()
	case "docs":
		return docsCommand(args[1:])
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
func docsCommand(args []string) error {
	if len(args) >= 1 && args[0] == "host" {
		listen := ":8081"
		if len(args) == 3 && args[1] == "--listen" {
			listen = args[2]
		} else if len(args) != 1 {
			return fmt.Errorf("usage: fluxa docs host [--listen <address>]")
		}
		fmt.Printf("Fluxa documentation: http://localhost%s\n", listen)
		return http.ListenAndServe(listen, fluxadocs.Handler())
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: fluxa docs <query> | fluxa docs host [--listen <address>]")
	}
	results, err := fluxadocs.Search(strings.Join(args, " "))
	if err != nil {
		return err
	}
	if len(results) == 0 {
		fmt.Println("No documentation matched. Try: fluxa docs lua")
		return nil
	}
	for _, document := range results {
		fmt.Printf("# %s\n\n%s\n", document.Name, document.Body)
	}
	return nil
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
	input := any(map[string]any{"trigger": "manual"})
	if len(args) == 3 && args[1] == "--input" {
		var value any
		if err := json.Unmarshal([]byte(args[2]), &value); err != nil {
			return fmt.Errorf("--input must be JSON: %w", err)
		}
		if object, ok := value.(map[string]any); ok {
			object["trigger"] = "manual"
			input = object
		} else {
			input = map[string]any{"trigger": "manual", "data": value}
		}
		args = args[:1]
	}
	if len(args) != 1 {
		return fmt.Errorf("usage: fluxa run <workflow> [--input <json>]")
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
	eid, err := r.Run(context.Background(), w.Root, args[0], wf, artifact, input)
	if err != nil {
		fmt.Println("Execution:", eid)
		return err
	}
	if tasks, taskErr := s.Tasks(context.Background(), eid); taskErr == nil {
		for _, task := range tasks {
			if task.Status == store.ExecutionCompleted {
				fmt.Println("✓", task.Name)
			}
		}
	}
	e, readErr := s.Execution(context.Background(), eid)
	if readErr != nil {
		return readErr
	}
	if e.Status == store.ExecutionWaiting {
		fmt.Println("\nWorkflow waiting")
		fmt.Println("Execution:", eid)
		return nil
	}
	fmt.Println("\nWorkflow completed")
	fmt.Println("Execution:", eid)
	if len(e.Result) > 0 {
		var value any
		if json.Unmarshal(e.Result, &value) == nil {
			b, _ := json.Marshal(value)
			fmt.Println("Result:", string(b))
		}
	}
	return nil
}
func runDaemon(args []string) error {
	listen := ":8080"
	if len(args) == 2 && args[0] == "--listen" {
		listen = args[1]
		args = nil
	}
	if len(args) != 0 {
		return fmt.Errorf("usage: fluxa daemon [--listen <address>]")
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Printf("Fluxa daemon listening on %s\n", listen)
	return daemon.New(w.Root, w.Manifest, s).Serve(ctx, listen)
}
func setActivation(command string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: fluxa %s <workflow>", command)
	}
	w, err := ws()
	if err != nil {
		return err
	}
	if _, ok := w.Manifest.Workflows[args[0]]; !ok {
		return fmt.Errorf("unknown workflow %q", args[0])
	}
	s, err := openStore(w)
	if err != nil {
		return err
	}
	defer s.Close()
	active := command == "activate"
	if err := s.SetWorkflowActive(context.Background(), args[0], active); err != nil {
		return err
	}
	state := "activated"
	if !active {
		state = "deactivated"
	}
	fmt.Printf("%s %s\n", args[0], state)
	return nil
}
func schedules() error {
	w, err := ws()
	if err != nil {
		return err
	}
	s, err := openStore(w)
	if err != nil {
		return err
	}
	defer s.Close()
	for name, workflow := range w.Manifest.Workflows {
		if workflow.Schedule.Cron == "" {
			continue
		}
		active, err := s.WorkflowActive(context.Background(), name)
		if err != nil {
			return err
		}
		next, err := s.ScheduleNextRun(context.Background(), name)
		if err != nil {
			return err
		}
		nextText := "not scheduled"
		if next != nil {
			nextText = next.UTC().Format(time.RFC3339)
		}
		state := "inactive"
		if active {
			state = "active"
		}
		fmt.Printf("%-24s %-8s %-18s %s\n", name, state, workflow.Schedule.Cron, nextText)
	}
	return nil
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
	fmt.Println("Execution", e.ID)
	fmt.Println("Workflow ", e.Workflow)
	fmt.Println("Status   ", e.Status)
	if e.Error != "" {
		fmt.Println("error:", e.Error)
	}
	if len(e.Result) > 0 {
		fmt.Println("result:", string(e.Result))
	}
	tasks, err := s.Tasks(context.Background(), e.ID)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		mark := "✗"
		if task.Status == store.ExecutionCompleted {
			mark = "✓"
		}
		fmt.Printf("%s %s\n", mark, task.Name)
		operations, opErr := s.Operations(context.Background(), task.ID)
		if opErr != nil {
			return opErr
		}
		for _, operation := range operations {
			var descriptor struct {
				Method string `json:"Method"`
				URL    string `json:"URL"`
			}
			_ = json.Unmarshal(operation.Descriptor, &descriptor)
			operationMark := "✗"
			if operation.Status == store.OperationCompleted {
				operationMark = "✓"
			}
			fmt.Printf("  %s %s %s %s\n", operationMark, strings.ToUpper(operation.Capability), descriptor.Method, descriptor.URL)
		}
	}
	return nil
}
func usage() {
	fmt.Fprintln(os.Stderr, `Fluxa — durable Lua automation

Usage: fluxa <command>
  init <directory>       create a workspace
  validate [workflow]    validate manifest and workflow selection
  workflows              list workflows
  run <workflow> [--input JSON] execute a workflow with optional input
  daemon [--listen ADDR] run schedules and webhook triggers
  activate <workflow>    enable schedule and webhook triggers
  deactivate <workflow>  disable schedule and webhook triggers
  schedules              list configured schedules
	  docs <query>           search bundled documentation
  docs host [--listen A] serve bundled documentation
  retry <execution-id> [--force] resume a failed execution
  runs [workflow]        list executions
  inspect <execution-id> show durable execution events`)
}
