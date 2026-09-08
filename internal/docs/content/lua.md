# Lua workflow API

The workflow file is executed as a top-level Lua chunk. Return its result
normally; Fluxa persists serializable returned values.

```lua
log.info("starting", { execution_id = fluxa.execution_id })

local response = task("fetch-user")
  :key("user-42")
  :retry(3)
  :timeout("10s")
  :run(function()
    return http.get("https://api.example.test/users/42")
  end)

local user = json.decode(response.body)
wait.sleep("10m")
return { user = user.name }
```

Globals: `fluxa`, `task`, `http`, `json`, `log`, `env`, and `wait`.
`fluxa` exposes `execution_id`, `workflow`, `attempt`, and `input`. `env` is an
empty table in the current release; secret/environment injection is deferred.

## task

`task(name)` returns a builder. `:key(value)`, `:retry(count)`, and
`:timeout(duration)` configure it; `:run(function() ... end)` executes it and
returns the normal Lua result. A builder cannot run twice. The shorthand
`task("name", function() ... end)` remains supported.

Use `:key` for dynamic loop instances. Without it, Fluxa matches the task by
deterministic invocation order and stops replay if that order changes.

Tasks are sequential. Lua computation outside or inside tasks is normal Lua;
only externally observable effects must use Fluxa capabilities. HTTP outside a
task is rejected.

## HTTP and JSON

Methods: `http.get`, `post`, `put`, `patch`, `delete`. The second argument can
contain `headers`, `query`, `json`, raw body fields, and timeout settings as
implemented by the HTTP capability. Responses are tables containing status,
headers, and body. Use `json.decode(response.body)` and `json.encode(value)`.

## Logging and waits

`log.info(message [, data])` writes a structured execution event.

`wait.sleep("10m")` stores a timer, marks the execution waiting, and exits the
Lua VM. `wait.until("2026-09-08T12:00:00Z")` accepts RFC3339 time. The daemon
later replays the chunk; the recorded wait resolves and preceding durable task
results are reused. `wait.for` is invalid native Lua because `for` is reserved;
`wait["for"]("10m")` exists but `wait.sleep` is canonical.
