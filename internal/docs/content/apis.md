# API cookbook

## Task builders

```lua
local value = task("name")
  :key("stable-item-id")     -- optional; required for dynamic loops
  :retry(3)                  -- immediate task retries in current runtime
  :timeout("20s")
  :run(function() return 42 end)
```

`task("name", function() ... end)` is shorthand without modifiers. Names label
inspection records; they are not globally unique identity. A task builder is
single-use.

## HTTP requests

```lua
local r = http.post("https://api.example.test/users", {
  headers = { Authorization = "Bearer token" },
  json = { name = "Ada", active = true },
  timeout = "10s"
})

if r.status ~= 201 then error("create failed: " .. r.status) end
local body = json.decode(r.body)
```

Supported verbs are GET, POST, PUT, PATCH, and DELETE. They are available only
inside `task`. Responses expose `status`, `headers`, and `body`; decode JSON
explicitly with `json.decode`.

## Runtime values and logging

```lua
log.info("started", { execution_id = fluxa.execution_id })
local payload = fluxa.input
return { workflow = fluxa.workflow, attempt = fluxa.attempt }
```

`fluxa.input` is a trigger envelope for daemon-triggered runs. Webhooks provide
`method`, `headers`, `query`, `body`, and optionally `json`; schedules provide
`trigger` and `scheduled_at`.

## Native Lua

Use tables, functions, loops, conditions, `string`, `table`, and normal Lua
business logic. Fluxa does not add nodes for mapping, dates, or JSON transforms.
The Lua VM intentionally exposes a limited standard library; filesystem,
subprocess, database, and arbitrary network libraries are not exposed.
