// Package runtime orchestrates durable workflow execution and replay.
package runtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fluxa-dev/fluxa/internal/capability/httpcap"
	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/store"
	lua "github.com/yuin/gopher-lua"
)

const APIVersion = 1

type Artifact struct {
	Version     string          `json:"version"`
	EntrySHA256 string          `json:"entry_sha256"`
	Config      json.RawMessage `json:"config"`
	RuntimeAPI  int             `json:"runtime_api"`
}
type Runner struct {
	Store *store.Store
	HTTP  *http.Client
}
type taskScope struct {
	id, instanceKey  string
	operationOrdinal int
}
type taskBuilder struct {
	name, key, timeout string
	retry              int
	executed           bool
}
type current struct {
	executionID, workflowVersion, workflowName string
	input                                      any
	attempt                                    int
	ctx                                        context.Context
	store                                      *store.Store
	replay, force, ambiguous                   bool
	taskStack                                  []taskScope
	nextByParent                               map[string]int
}

func New(s *store.Store) *Runner {
	return &Runner{Store: s, HTTP: &http.Client{Timeout: 30 * time.Second}}
}
func id(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
func digest(v []byte) string { h := sha256.Sum256(v); return hex.EncodeToString(h[:]) }
func ArtifactFor(root, name string, w config.Workflow) (Artifact, error) {
	entry := filepath.Join(root, w.Entry)
	b, err := os.ReadFile(entry)
	if err != nil {
		return Artifact{}, err
	}
	cfg, _ := json.Marshal(struct {
		Name     string          `json:"name"`
		Workflow config.Workflow `json:"workflow"`
	}{name, w})
	a := Artifact{EntrySHA256: digest(b), Config: cfg, RuntimeAPI: APIVersion}
	raw, _ := json.Marshal(a)
	a.Version = digest(raw)
	return a, nil
}
func Version(entry string) string { b, _ := os.ReadFile(entry); return digest(b) } // compatibility for callers; ArtifactFor is used by the CLI.
func Compile(entry string) error {
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	_, err := L.LoadFile(entry)
	return err
}

func (r *Runner) Run(ctx context.Context, root, name string, w config.Workflow, artifact Artifact, input any) (string, error) {
	eid := id("exec_")
	if err := r.Store.CreateExecution(ctx, store.Execution{ID: eid, Workflow: name, Version: artifact.Version, Status: store.ExecutionRunning}, input, artifact); err != nil {
		return "", err
	}
	_ = r.Store.Event(ctx, eid, "", "", "execution.started", map[string]any{"workflow": name, "version": artifact.Version})
	return eid, r.execute(ctx, root, name, w, artifact, eid, input, false, false, 0)
}
func (r *Runner) Retry(ctx context.Context, root string, m config.Manifest, executionID string, force bool) (string, error) {
	e, err := r.Store.Execution(ctx, executionID)
	if err != nil {
		return "", err
	}
	if e.Status != store.ExecutionFailed && e.Status != store.ExecutionInterrupted && e.Status != store.ExecutionPausedAmbiguous {
		return "", fmt.Errorf("execution %s is not retryable (status %s)", executionID, e.Status)
	}
	w, ok := m.Workflows[e.Workflow]
	if !ok {
		return "", fmt.Errorf("original workflow %q no longer exists", e.Workflow)
	}
	artifact, err := ArtifactFor(root, e.Workflow, w)
	if err != nil {
		return "", err
	}
	if artifact.Version != e.Version {
		return "", fmt.Errorf("workflow_version_mismatch: execution uses %s but current source is %s", e.Version, artifact.Version)
	}
	if e.Status == store.ExecutionPausedAmbiguous && !force {
		return "", fmt.Errorf("execution has ambiguous operation; retry requires --force")
	}
	var input any
	if len(e.Input) > 0 {
		if err := json.Unmarshal(e.Input, &input); err != nil {
			return "", fmt.Errorf("decode original input: %w", err)
		}
	}
	attempt, err := r.Store.BeginRecovery(ctx, executionID, e.Version, force)
	if err != nil {
		return "", err
	}
	_ = r.Store.Event(ctx, executionID, "", "", "execution.recovery_started", map[string]any{"attempt": attempt, "force": force})
	err = r.execute(ctx, root, e.Workflow, w, artifact, executionID, input, true, force, attempt)
	return executionID, err
}

func (r *Runner) execute(ctx context.Context, root, name string, w config.Workflow, artifact Artifact, eid string, input any, replay, force bool, recoveryAttempt int) error {
	if w.Timeout != "" {
		d, err := time.ParseDuration(w.Timeout)
		if err != nil {
			return err
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	lua.OpenBase(L)
	lua.OpenTable(L)
	lua.OpenString(L)
	lua.OpenMath(L)
	c := &current{executionID: eid, workflowVersion: artifact.Version, workflowName: name, input: input, attempt: recoveryAttempt + 1, ctx: ctx, store: r.Store, replay: replay, force: force, nextByParent: map[string]int{}}
	r.bind(L, c)
	entry := filepath.Join(root, w.Entry)
	if err := L.DoFile(entry); err != nil {
		return r.finish(eid, recoveryAttempt, store.ExecutionFailed, err.Error())
	}
	ret := toGo(L.Get(-1))
	L.Pop(1)
	if err := r.Store.SaveExecutionResult(ctx, eid, ret); err != nil {
		return r.finish(eid, recoveryAttempt, store.ExecutionFailed, err.Error())
	}
	_ = r.Store.Event(ctx, eid, "", "", "execution.completed", map[string]any{"result": ret, "replay": replay})
	return r.finish(eid, recoveryAttempt, store.ExecutionCompleted, "")
}
func (r *Runner) finish(eid string, attempt int, status, msg string) error {
	ctx := context.Background()
	_ = r.Store.FinishExecution(ctx, eid, status, msg)
	if attempt > 0 {
		_ = r.Store.FinishRecovery(ctx, eid, attempt, status, msg)
	}
	return errorFor(status, msg)
}
func errorFor(status, msg string) error {
	if status == store.ExecutionCompleted {
		return nil
	}
	return fmt.Errorf("%s: %s", status, msg)
}

func (r *Runner) bind(L *lua.LState, c *current) {
	r.bindTaskBuilder(L, c)
	L.SetGlobal("task", L.NewFunction(func(L *lua.LState) int { return r.taskCall(L, c) }))
	log := L.NewTable()
	L.SetField(log, "info", L.NewFunction(func(L *lua.LState) int {
		msg := L.CheckString(1)
		var data any
		if L.GetTop() > 1 {
			data = toGo(L.Get(2))
		}
		scope := c.scope()
		_ = c.store.Event(c.ctx, c.executionID, scope.id, "", "log.info", map[string]any{"message": msg, "data": data})
		return 0
	}))
	L.SetGlobal("log", log)
	h := L.NewTable()
	for _, method := range []string{"get", "post", "put", "patch", "delete"} {
		m := method
		L.SetField(h, m, L.NewFunction(func(L *lua.LState) int { return r.httpCall(L, c, strings.ToUpper(m)) }))
	}
	L.SetGlobal("http", h)
	j := L.NewTable()
	L.SetField(j, "decode", L.NewFunction(func(L *lua.LState) int {
		var v any
		if err := json.Unmarshal([]byte(L.CheckString(1)), &v); err != nil {
			L.RaiseError("%s", err)
			return 0
		}
		L.Push(fromGo(L, v))
		return 1
	}))
	L.SetField(j, "encode", L.NewFunction(func(L *lua.LState) int {
		b, err := json.Marshal(toGo(L.Get(1)))
		if err != nil {
			L.RaiseError("%s", err)
			return 0
		}
		L.Push(lua.LString(b))
		return 1
	}))
	L.SetGlobal("json", j)
	L.SetGlobal("env", L.NewTable())
	fluxa := L.NewTable()
	L.SetField(fluxa, "execution_id", lua.LString(c.executionID))
	L.SetField(fluxa, "workflow", lua.LString(c.workflowName))
	L.SetField(fluxa, "attempt", lua.LNumber(c.attempt))
	L.SetField(fluxa, "input", fromGo(L, c.input))
	L.SetGlobal("fluxa", fluxa)
}

const taskBuilderType = "fluxa.task_builder"

func (r *Runner) bindTaskBuilder(L *lua.LState, c *current) {
	mt := L.NewTypeMetatable(taskBuilderType)
	methods := L.NewTable()
	L.SetField(methods, "key", L.NewFunction(func(L *lua.LState) int {
		b := builderFrom(L)
		ensureUnexecuted(L, b)
		v := L.Get(2)
		if v == lua.LNil {
			L.RaiseError("task key cannot be nil")
			return 0
		}
		b.key = v.String()
		L.Push(L.Get(1))
		return 1
	}))
	L.SetField(methods, "retry", L.NewFunction(func(L *lua.LState) int {
		b := builderFrom(L)
		ensureUnexecuted(L, b)
		n := L.CheckInt(2)
		if n < 0 {
			L.RaiseError("task retry must be non-negative")
			return 0
		}
		b.retry = n
		L.Push(L.Get(1))
		return 1
	}))
	L.SetField(methods, "timeout", L.NewFunction(func(L *lua.LState) int {
		b := builderFrom(L)
		ensureUnexecuted(L, b)
		d := L.CheckString(2)
		if _, err := time.ParseDuration(d); err != nil {
			L.RaiseError("invalid task timeout %q", d)
			return 0
		}
		b.timeout = d
		L.Push(L.Get(1))
		return 1
	}))
	L.SetField(methods, "run", L.NewFunction(func(L *lua.LState) int {
		b := builderFrom(L)
		ensureUnexecuted(L, b)
		b.executed = true
		return r.runTask(L, c, b, L.CheckFunction(2))
	}))
	L.SetField(mt, "__index", methods)
}
func builderFrom(L *lua.LState) *taskBuilder {
	ud := L.CheckUserData(1)
	b, ok := ud.Value.(*taskBuilder)
	if !ok {
		L.ArgError(1, "expected Fluxa task builder")
	}
	return b
}
func ensureUnexecuted(L *lua.LState, b *taskBuilder) {
	if b.executed {
		L.RaiseError("task builder %q was already executed", b.name)
	}
}
func (r *Runner) newBuilder(L *lua.LState, name string) *lua.LUserData {
	ud := L.NewUserData()
	ud.Value = &taskBuilder{name: name}
	L.SetMetatable(ud, L.GetTypeMetatable(taskBuilderType))
	return ud
}
func (c *current) scope() taskScope {
	if len(c.taskStack) == 0 {
		return taskScope{}
	}
	return c.taskStack[len(c.taskStack)-1]
}
func taskIdentity(version, parent, name, key string, ordinal int) (string, string) {
	mode := "ordinal"
	token := fmt.Sprintf("ordinal:%d", ordinal)
	if key != "" {
		mode = "key"
		token = "key:" + key
	}
	return digest([]byte(strings.Join([]string{"fluxa.task.v1", version, parent, name, token}, "\n"))), mode
}
func (r *Runner) taskCall(L *lua.LState, c *current) int {
	name := L.CheckString(1)
	if L.GetTop() == 1 {
		L.Push(r.newBuilder(L, name))
		return 1
	}
	b := &taskBuilder{name: name, executed: true}
	if L.GetTop() == 2 {
		return r.runTask(L, c, b, L.CheckFunction(2))
	}
	options, ok := L.Get(2).(*lua.LTable)
	if !ok {
		L.ArgError(2, "expected task options table or function")
		return 0
	}
	if v := L.GetField(options, "key"); v != lua.LNil {
		b.key = v.String()
	}
	if v := L.GetField(options, "retry"); v != lua.LNil {
		n, ok := v.(lua.LNumber)
		if !ok || n < 0 || n != lua.LNumber(int(n)) {
			L.RaiseError("task retry must be a non-negative integer")
			return 0
		}
		b.retry = int(n)
	}
	if v := L.GetField(options, "timeout"); v != lua.LNil {
		b.timeout = v.String()
		if _, err := time.ParseDuration(b.timeout); err != nil {
			L.RaiseError("invalid task timeout %q", b.timeout)
			return 0
		}
	}
	return r.runTask(L, c, b, L.CheckFunction(3))
}
func (r *Runner) runTask(L *lua.LState, c *current, b *taskBuilder, fn *lua.LFunction) int {
	name, key := b.name, b.key
	parent := c.scope().instanceKey
	ordinal := c.nextByParent[parent]
	c.nextByParent[parent]++
	instance, mode := taskIdentity(c.workflowVersion, parent, name, key, ordinal)
	taskID := id("task_")
	existing, err := c.store.TaskByInstance(c.ctx, c.executionID, instance)
	if err == nil {
		if !c.replay {
			L.RaiseError("task_identity_conflict: duplicate task instance %s", instance)
			return 0
		}
		if existing.Name != name || existing.TaskKey != key || existing.IdentityMode != mode {
			L.RaiseError("task_identity_conflict: %s", instance)
			return 0
		}
		taskID = existing.ID
		if c.replay && existing.Status == store.ExecutionCompleted {
			var value any
			if json.Unmarshal(existing.Result, &value) != nil {
				L.RaiseError("persisted task result is invalid")
				return 0
			}
			_ = c.store.Event(c.ctx, c.executionID, taskID, "", "task.replayed", map[string]any{"instance_key": instance})
			L.Push(fromGo(L, value))
			return 1
		}
		if c.replay {
			if err := c.store.RestartTask(c.ctx, taskID); err != nil {
				L.RaiseError("%s", err)
				return 0
			}
		}
	} else if err == sql.ErrNoRows {
		if err := c.store.CreateTask(c.ctx, store.Task{ID: taskID, ExecutionID: c.executionID, InstanceKey: instance, ParentInstanceKey: parent, Name: name, TaskKey: key, InvocationOrdinal: ordinal, IdentityMode: mode, Status: store.ExecutionRunning, Attempt: 1}); err != nil {
			L.RaiseError("%s", err)
			return 0
		}
	} else {
		L.RaiseError("%s", err)
		return 0
	}
	for attempt := 0; ; attempt++ {
		taskCtx := c.ctx
		var cancel context.CancelFunc
		if b.timeout != "" {
			d, _ := time.ParseDuration(b.timeout)
			taskCtx, cancel = context.WithTimeout(c.ctx, d)
		}
		previousCtx := c.ctx
		c.ctx = taskCtx
		c.taskStack = append(c.taskStack, taskScope{id: taskID, instanceKey: instance})
		_ = c.store.Event(c.ctx, c.executionID, taskID, "", "task.started", map[string]any{"name": name, "instance_key": instance, "replay": c.replay, "attempt": attempt + 1})
		err = L.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true})
		c.taskStack = c.taskStack[:len(c.taskStack)-1]
		c.ctx = previousCtx
		if cancel != nil {
			cancel()
		}
		if err == nil {
			ret := L.Get(-1)
			L.Pop(1)
			result := toGo(ret)
			_ = c.store.FinishTask(c.ctx, taskID, store.ExecutionCompleted, "", result)
			_ = c.store.Event(c.ctx, c.executionID, taskID, "", "task.completed", map[string]any{"result": result})
			L.Push(ret)
			return 1
		}
		_ = c.store.FinishTask(context.Background(), taskID, store.ExecutionFailed, err.Error(), nil)
		_ = c.store.Event(context.Background(), c.executionID, taskID, "", "task.failed", map[string]any{"error": err.Error(), "attempt": attempt + 1})
		if c.ambiguous || attempt >= b.retry {
			L.RaiseError("%s", err)
			return 0
		}
		if restartErr := c.store.RestartTask(c.ctx, taskID); restartErr != nil {
			L.RaiseError("%s", restartErr)
			return 0
		}
		_ = c.store.Event(c.ctx, c.executionID, taskID, "", "task.retrying", map[string]any{"next_attempt": attempt + 2})
	}
}
func (r *Runner) ctxTable(L *lua.LState, id string, input any) *lua.LTable {
	t := L.NewTable()
	L.SetField(t, "execution_id", lua.LString(id))
	L.SetField(t, "input", fromGo(L, input))
	return t
}
func (r *Runner) httpCall(L *lua.LState, c *current, method string) int {
	scope := c.scope()
	if scope.id == "" {
		L.RaiseError("http capability may only be called inside task")
		return 0
	}
	request, err := requestFromLua(L, method)
	if err != nil {
		L.RaiseError("%s", err)
		return 0
	}
	scope.operationOrdinal++
	c.taskStack[len(c.taskStack)-1] = scope
	descriptor, fingerprint, err := httpcap.Canonicalize(request, scope.instanceKey, scope.operationOrdinal)
	if err != nil {
		L.RaiseError("%s", err)
		return 0
	}
	descriptorJSON, _ := json.Marshal(descriptor)
	op, err := c.store.OperationAt(c.ctx, scope.id, scope.operationOrdinal)
	if err == nil {
		if op.Fingerprint != fingerprint {
			L.RaiseError("operation_fingerprint_mismatch at ordinal %d", scope.operationOrdinal)
			return 0
		}
		if op.Status == store.OperationCompleted {
			var v any
			if json.Unmarshal(op.Result, &v) != nil {
				L.RaiseError("persisted operation result is invalid")
				return 0
			}
			_ = c.store.Event(c.ctx, c.executionID, scope.id, op.ID, "operation.replayed", map[string]any{"ordinal": scope.operationOrdinal})
			L.Push(fromGo(L, v))
			return 1
		}
		if op.Status == store.OperationAmbiguous && !c.force {
			c.ambiguous = true
			L.RaiseError("ambiguous operation requires --force")
			return 0
		}
		if op.Status == store.OperationRunning && httpUnsafe(method) && !c.force {
			c.ambiguous = true
			L.RaiseError("interrupted unsafe operation requires --force")
			return 0
		}
		op.Attempt++
		if err := c.store.StartOperationAttempt(c.ctx, op.ID, op.Attempt, "replay"); err != nil {
			L.RaiseError("%s", err)
			return 0
		}
	} else if err == sql.ErrNoRows {
		key := ""
		if request.UseIdempotency {
			key = digest([]byte(c.executionID + "\n" + scope.instanceKey + fmt.Sprintf("\n%d", scope.operationOrdinal)))
		}
		op = store.Operation{ID: id("op_"), TaskID: scope.id, Ordinal: scope.operationOrdinal, Fingerprint: fingerprint, Capability: "http", Attempt: 1, Descriptor: descriptorJSON, IdempotencyKey: key}
		if err := c.store.CreateOperation(c.ctx, op); err != nil {
			L.RaiseError("%s", err)
			return 0
		}
		if err := c.store.StartOperationAttempt(c.ctx, op.ID, 1, "dispatch"); err != nil {
			L.RaiseError("%s", err)
			return 0
		}
	} else {
		L.RaiseError("%s", err)
		return 0
	}
	response, dispatchErr := httpcap.Dispatch(c.ctx, r.HTTP, request, op.IdempotencyKey)
	if dispatchErr != nil {
		status := store.OperationFailed
		if dispatchErr.Ambiguous {
			status = store.OperationAmbiguous
			c.ambiguous = true
		}
		_ = c.store.FinishOperation(context.Background(), op.ID, status, nil, string(dispatchErr.Kind), dispatchErr.Error())
		_ = c.store.Event(context.Background(), c.executionID, scope.id, op.ID, "operation."+status, map[string]any{"error": dispatchErr.Error(), "kind": dispatchErr.Kind})
		L.RaiseError("http %s: %s", status, dispatchErr)
		return 0
	}
	_ = c.store.FinishOperation(c.ctx, op.ID, store.OperationCompleted, response, "", "")
	_ = c.store.Event(c.ctx, c.executionID, scope.id, op.ID, "operation.completed", map[string]any{"status": response.Status, "ordinal": scope.operationOrdinal})
	L.Push(fromGo(L, map[string]any{"status": float64(response.Status), "body": response.Body, "headers": headersToMap(response.Headers)}))
	return 1
}
func requestFromLua(L *lua.LState, method string) (httpcap.Request, error) {
	r := httpcap.Request{Method: method, URL: L.CheckString(1), Headers: map[string]string{}, IdempotencyHeader: "Idempotency-Key"}
	if L.GetTop() < 2 {
		return r, nil
	}
	opts, ok := L.Get(2).(*lua.LTable)
	if !ok {
		return r, fmt.Errorf("http options must be a table")
	}
	if v := L.GetField(opts, "body"); v != lua.LNil {
		r.Body = []byte(v.String())
	}
	if v := L.GetField(opts, "json"); v != lua.LNil {
		b, err := json.Marshal(toGo(v))
		if err != nil {
			return r, err
		}
		r.Body = b
		r.Headers["Content-Type"] = "application/json"
	}
	if ht, ok := L.GetField(opts, "headers").(*lua.LTable); ok {
		ht.ForEach(func(k, v lua.LValue) { r.Headers[k.String()] = v.String() })
	}
	v := L.GetField(opts, "idempotency")
	if v != lua.LNil {
		r.UseIdempotency = true
		if t, ok := v.(*lua.LTable); ok {
			if h := L.GetField(t, "header"); h != lua.LNil {
				r.IdempotencyHeader = h.String()
			}
		}
	}
	return r, nil
}
func httpUnsafe(m string) bool { return m != "GET" && m != "HEAD" && m != "OPTIONS" }
func headersToMap(h http.Header) map[string]any {
	out := map[string]any{}
	for k, v := range h {
		a := make([]any, len(v))
		for i, x := range v {
			a[i] = x
		}
		out[k] = a
	}
	return out
}
func toGo(v lua.LValue) any {
	switch x := v.(type) {
	case *lua.LTable:
		m := map[string]any{}
		arr := []any{}
		isArr := true
		n := 0
		x.ForEach(func(k, v lua.LValue) {
			if i, ok := k.(lua.LNumber); ok && int(i) == n+1 {
				n++
				arr = append(arr, toGo(v))
			} else {
				isArr = false
				m[k.String()] = toGo(v)
			}
		})
		if isArr {
			return arr
		}
		return m
	case lua.LString:
		return string(x)
	case lua.LNumber:
		return float64(x)
	case lua.LBool:
		return bool(x)
	case *lua.LNilType:
		return nil
	default:
		return v.String()
	}
}
func fromGo(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case map[string]any:
		t := L.NewTable()
		for k, v := range x {
			t.RawSetString(k, fromGo(L, v))
		}
		return t
	case []any:
		t := L.NewTable()
		for i, v := range x {
			t.RawSetInt(i+1, fromGo(L, v))
		}
		return t
	case string:
		return lua.LString(x)
	case float64:
		return lua.LNumber(x)
	case bool:
		return lua.LBool(x)
	case nil:
		return lua.LNil
	default:
		return lua.LString(fmt.Sprint(v))
	}
}
