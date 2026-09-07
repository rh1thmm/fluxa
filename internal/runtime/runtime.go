package runtime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fluxa-dev/fluxa/internal/config"
	"github.com/fluxa-dev/fluxa/internal/store"
	lua "github.com/yuin/gopher-lua"
)

type Runner struct {
	Store *store.Store
	HTTP  *http.Client
}
type current struct {
	executionID string
	taskID      string
	ordinal     int
	ctx         context.Context
	store       *store.Store
}

func New(s *store.Store) *Runner {
	return &Runner{Store: s, HTTP: &http.Client{Timeout: 30 * time.Second}}
}
func id(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
func hash(v string) string { h := sha256.Sum256([]byte(v)); return hex.EncodeToString(h[:]) }

func (r *Runner) Run(ctx context.Context, root string, name string, w config.Workflow, version string, input any) (string, error) {
	execID := id("exec_")
	if err := r.Store.CreateExecution(ctx, store.Execution{ID: execID, Workflow: name, Version: version, Status: "running"}, input); err != nil {
		return "", err
	}
	_ = r.Store.Event(ctx, execID, "", "", "execution.started", map[string]any{"workflow": name})
	if w.Timeout != "" {
		d, _ := time.ParseDuration(w.Timeout)
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
	cur := &current{executionID: execID, ctx: ctx, store: r.Store}
	r.bind(L, cur)
	entry := filepath.Join(root, w.Entry)
	if err := L.DoFile(entry); err != nil {
		_ = r.Store.FinishExecution(context.Background(), execID, "failed", err.Error())
		return execID, err
	}
	fn, ok := L.Get(-1).(*lua.LFunction)
	L.Pop(1)
	if !ok {
		err := fmt.Errorf("workflow %q must return a function", name)
		_ = r.Store.FinishExecution(context.Background(), execID, "failed", err.Error())
		return execID, err
	}
	if err := L.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true}, r.ctxTable(L, execID)); err != nil {
		status := "failed"
		if strings.Contains(err.Error(), "ambiguous") {
			status = "paused_ambiguous"
		}
		_ = r.Store.Event(context.Background(), execID, "", "", "execution."+status, map[string]any{"error": err.Error()})
		_ = r.Store.FinishExecution(context.Background(), execID, status, err.Error())
		return execID, err
	}
	ret := toGo(L.Get(-1))
	L.Pop(1)
	_ = r.Store.Event(ctx, execID, "", "", "execution.completed", map[string]any{"result": ret})
	_ = r.Store.FinishExecution(ctx, execID, "completed", "")
	return execID, nil
}
func (r *Runner) bind(L *lua.LState, c *current) {
	L.SetGlobal("task", L.NewFunction(func(L *lua.LState) int {
		name := L.CheckString(1)
		idx := 2
		var key string
		if L.GetTop() >= 3 {
			if t, ok := L.Get(2).(*lua.LTable); ok {
				key = L.GetField(t, "key").String()
			}
			idx = 3
		}
		fn := L.CheckFunction(idx)
		taskID := id("task_")
		if err := c.store.CreateTask(c.ctx, store.Task{ID: taskID, ExecutionID: c.executionID, Name: name, TaskKey: key, Status: "running", Attempt: 1}); err != nil {
			L.RaiseError("%s", err.Error())
			return 0
		}
		old := c.taskID
		c.taskID = taskID
		c.ordinal = 0
		_ = c.store.Event(c.ctx, c.executionID, taskID, "", "task.started", map[string]any{"name": name, "key": key})
		err := L.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true})
		if err != nil {
			c.taskID = old
			_ = c.store.FinishTask(context.Background(), taskID, "failed", err.Error())
			_ = c.store.Event(context.Background(), c.executionID, taskID, "", "task.failed", map[string]any{"error": err.Error()})
			L.RaiseError("%s", err.Error())
			return 0
		}
		ret := L.Get(-1)
		L.Pop(1)
		c.taskID = old
		_ = c.store.FinishTask(c.ctx, taskID, "completed", "")
		_ = c.store.Event(c.ctx, c.executionID, taskID, "", "task.completed", map[string]any{"result": toGo(ret)})
		L.Push(ret)
		return 1
	}))
	log := L.NewTable()
	L.SetField(log, "info", L.NewFunction(func(L *lua.LState) int {
		msg := L.CheckString(1)
		data := any(nil)
		if L.GetTop() > 1 {
			data = toGo(L.Get(2))
		}
		_ = c.store.Event(c.ctx, c.executionID, c.taskID, "", "log.info", map[string]any{"message": msg, "data": data})
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
			L.RaiseError("%s", err.Error())
			return 0
		}
		L.Push(fromGo(L, v))
		return 1
	}))
	L.SetField(j, "encode", L.NewFunction(func(L *lua.LState) int {
		b, err := json.Marshal(toGo(L.Get(1)))
		if err != nil {
			L.RaiseError("%s", err.Error())
			return 0
		}
		L.Push(lua.LString(b))
		return 1
	}))
	L.SetGlobal("json", j)
	L.SetGlobal("env", L.NewTable())
}
func (r *Runner) ctxTable(L *lua.LState, id string) *lua.LTable {
	t := L.NewTable()
	L.SetField(t, "execution_id", lua.LString(id))
	return t
}
func (r *Runner) httpCall(L *lua.LState, c *current, method string) int {
	if c.taskID == "" {
		L.RaiseError("http capability may only be called inside task")
		return 0
	}
	url := L.CheckString(1)
	var body io.Reader
	headers := map[string]string{}
	if L.GetTop() > 1 {
		if opts, ok := L.Get(2).(*lua.LTable); ok {
			if v := L.GetField(opts, "body"); v != lua.LNil {
				body = strings.NewReader(v.String())
			}
			if v := L.GetField(opts, "json"); v != lua.LNil {
				b, _ := json.Marshal(toGo(v))
				body = strings.NewReader(string(b))
				headers["Content-Type"] = "application/json"
			}
			if ht, ok := L.GetField(opts, "headers").(*lua.LTable); ok {
				ht.ForEach(func(k, v lua.LValue) { headers[k.String()] = v.String() })
			}
		}
	}
	c.ordinal++
	fp := hash(method + "\n" + url + fmt.Sprint(headers))
	op := store.Operation{ID: id("op_"), TaskID: c.taskID, Ordinal: c.ordinal, Fingerprint: fp, Capability: "http", Attempt: 1}
	if err := c.store.BeginOperation(c.ctx, op); err != nil {
		L.RaiseError("%s", err.Error())
		return 0
	}
	req, err := http.NewRequestWithContext(c.ctx, method, url, body)
	if err == nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		var resp *http.Response
		resp, err = r.HTTP.Do(req)
		if err == nil {
			defer resp.Body.Close()
			b, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			if readErr != nil {
				err = readErr
			} else {
				result := map[string]any{"status": resp.StatusCode, "body": string(b), "headers": resp.Header}
				_ = c.store.FinishOperation(c.ctx, op.ID, "completed", result, "")
				_ = c.store.Event(c.ctx, c.executionID, c.taskID, op.ID, "operation.completed", map[string]any{"status": resp.StatusCode})
				L.Push(fromGo(L, result))
				return 1
			}
		}
	}
	status := "failed"
	if method != "GET" && method != "HEAD" {
		status = "ambiguous"
	}
	_ = c.store.FinishOperation(context.Background(), op.ID, status, nil, err.Error())
	_ = c.store.Event(context.Background(), c.executionID, c.taskID, op.ID, "operation."+status, map[string]any{"error": err.Error()})
	L.RaiseError("http %s: %s", status, err)
	return 0
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
func Version(entry string) string { b, _ := os.ReadFile(entry); return hash(string(b)) }

// Compile checks Lua syntax without executing workspace code or capabilities.
func Compile(entry string) error {
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()
	_, err := L.LoadFile(entry)
	return err
}
