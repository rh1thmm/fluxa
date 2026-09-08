# Tutorials and recipes

## Scheduled API digest

Manifest:

```toml
[workflows.digest]
entry = "workflows/digest.lua"

[workflows.digest.schedule]
cron = "0 9 * * 1-5"
timezone = "America/Edmonton"
```

Workflow:

```lua
local response = task("fetch-status")
  :retry(2)
  :run(function()
    return http.get("https://status.example.test/api")
  end)

local status = json.decode(response.body)
log.info("daily status", { state = status.state })
return status
```

Activate it with `fluxa activate digest`, then run `fluxa daemon`. Deactivation
stops schedule and webhook starts but preserves manual execution.

## Webhook receiver

```toml
[workflows.incoming]
entry = "workflows/incoming.lua"

[workflows.incoming.webhook]
method = "POST"
path = "/incoming"
```

```lua
local event = fluxa.input.json
if not event then error("expected JSON webhook") end

return task("record-event")
  :key(event.id)
  :run(function()
    return http.post("https://api.example.test/events", { json = event })
  end)
```

Run the daemon and post JSON to `/incoming`. Fluxa commits a queued execution
before replying `202`; inspect it by returned execution ID.

## Dynamic item processing

```lua
for _, lead in ipairs(leads) do
  task("send-email")
    :key(lead.id)
    :retry(3)
    :run(function()
      return http.post("https://email.example.test/send", {
        json = { to = lead.email, subject = "Hello " .. lead.name }
      })
    end)
end
```

Keys make each loop task stable across replay. The current runtime is sequential;
bounded parallel processing is planned, not silently inferred.

## Pause without keeping a process alive

```lua
task("request-export"):run(function()
  return http.post("https://api.example.test/exports", { json = { kind = "daily" } })
end)

wait.sleep("15m")

return task("fetch-export"):run(function()
  return http.get("https://api.example.test/exports/latest")
end)
```

The first HTTP result is replayed rather than resubmitted when the daemon later
resumes the timer.
