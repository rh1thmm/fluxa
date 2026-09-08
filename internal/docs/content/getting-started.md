# Getting started

## 1. Build and create a workspace

```sh
make build
./bin/fluxa init hello-fluxa
cd hello-fluxa
../bin/fluxa validate
../bin/fluxa run example
```

`Fluxa.toml` names workflows and controls their lifecycle. The Lua file is the
workflow body—there is no registration wrapper or graph editor.

## 2. Write a durable HTTP workflow

```lua
log.info("fetching a post")

local response = task("fetch-post")
  :timeout("10s")
  :run(function()
    return http.get("https://jsonplaceholder.typicode.com/posts/1")
  end)

local post = json.decode(response.body)
return { id = post.id, title = post.title }
```

Run it with `fluxa run example`; inspect its durable history with `fluxa inspect
<execution-id>`. The `task` is where HTTP becomes durable. Transforming `post`
is ordinary Lua and does not need a task.

## 3. Start the daemon

Add a schedule or webhook to the manifest, then run:

```sh
fluxa daemon --listen :8080
```

The daemon owns local queue dispatch, cron checks, due timers, and webhooks.
Use a process supervisor in production; native service installation is not yet
implemented.

Next: [tutorials](/docs/tutorials), [Lua API](/docs/lua), and
[durability](/docs/durability).
