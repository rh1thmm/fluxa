# Fluxa.toml reference

Unknown fields are rejected. This keeps the manifest a reliable deployment and
lifecycle contract.

```toml
[workspace]
api_version = 1
default_timezone = "UTC"
default_environment = "development"

[workflows.outreach]
entry = "workflows/outreach.lua"
timeout = "30s"
max_concurrency = 1
on_failure = "outreach-error"
secrets = ["email.production"]

[workflows.outreach.schedule]
cron = "0 9 * * 1-5"
timezone = "America/Edmonton"

[workflows.outreach.webhook]
method = "POST"
path = "/outreach"
```

`workspace.api_version` must be `1`. `entry` must be a relative path inside the
workspace and must exist. `timeout` uses Go duration syntax such as `20s` or
`10m`. `max_concurrency` is validated but the queue currently enforces it only
for daemon-dispatched executions.

`schedule.cron` uses standard five-field cron syntax. Timezone defaults to UTC
when omitted. Missed schedule intervals are coalesced: Fluxa runs the earliest
due slot once and then advances to the next slot after the current time, rather
than emitting a potentially dangerous catch-up burst.

Webhook paths begin with `/`; method/path pairs must be unique. JSON request
bodies are decoded when the content type contains `application/json`; all bodies
are limited to 4 MiB.

`on_failure`, `secrets`, `defaults.retry`, `default_environment`, and
`default_timezone` are schema-recognized but not all have runtime behavior yet.
They are reserved configuration surface, not a promise of secret or retry
policy support.
