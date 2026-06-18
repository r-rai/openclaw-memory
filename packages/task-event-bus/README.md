# TaskEventBus

Redis-backed unified task queue + event pub/sub for OpenClaw agents.

- **Event bus** — Redis pub/sub for inter-agent real-time events
- **Task queue** — Async task dispatch with correlation IDs and result polling
- **Worker** — Standalone Docker container (survives OpenClaw restarts)

## Tools

| Tool | Description |
|------|-------------|
| `event_publish` | Publish an event to a pub/sub channel |
| `event_subscribe` | Subscribe to a channel (one-shot, with timeout) |
| `task_dispatch` | Dispatch a task to a queue, get a `correlationId` |
| `task_results` | Poll for a task result by `correlationId` |

## Installation

```bash
pnpm add @openclaw-memory/task-event-bus
```

## OpenClaw Configuration

```json
{
  "plugins": {
    "entries": {
      "task-event-bus": "/path/to/node_modules/@openclaw-memory/task-event-bus"
    },
    "load": {
      "paths": ["/path/to/node_modules/@openclaw-memory/task-event-bus"]
    }
  }
}
```

### Plugin Configuration

You can configure the plugin in `openclaw.json`:

```json
{
  "task-event-bus": {
    "redisUrl": "redis://redis:6379",
    "resultTtlMs": 300000,
    "channelPrefix": "teb",
    "queuePrefix": "tasks",
    "resultPrefix": "teb:result"
  }
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `redisUrl` | `redis://redis:6379` | Redis connection URL |
| `resultTtlMs` | `300000` | TTL for task results in Redis (in milliseconds) |
| `channelPrefix` | `teb` | Redis key prefix for event pub/sub channels |
| `queuePrefix` | `tasks` | Redis key prefix for task queues |
| `resultPrefix` | `teb:result` | Redis key prefix for task results (must match the worker's `TEB_RESULT_PREFIX`) |

## Event Bus

### Publish an event

```js
await event_publish({
  channel: "events:agent.ui",   // or just "my-channel" (prefix "teb:" added automatically)
  payload: { type: "task.completed", taskId: "abc123" }
});
// → { channel: "teb:events:agent.ui", subscriberCount: 0, payload: {...} }
```

### Subscribe to an event

```js
await event_subscribe({
  channel: "events:agent.ui",
  timeoutMs: 5000   // wait up to 5s for a message
});
// → { channel: "teb:events:agent.ui", message: {...}, timedOut: false }
```

## Task Queue

### Dispatch a task

```js
const { correlationId } = await task_dispatch({
  queue: "code",           // general | code | qa | research | ui
  payload: {
    type: "code-run",      // code-run | web-fetch | agent-run | custom
    command: "echo hello",
    timeoutSeconds: 30
  },
  priority: 0               // 1-10 (1=highest), 0 = standard FIFO
});
// → { correlationId: "teb-123456-abc123", queue: "code", dispatchedAt: "..." }
```

### Poll for result

```js
const result = await task_results({
  correlationId: "teb-123456-abc123",
  wait: 10000              // poll for up to 10s
});
// → { status: "completed", result: { success: true, stdout: "hello\n", ... } }
```

## Docker Worker

Run as a separate container so tasks keep processing even if OpenClaw restarts:

```yaml
services:
  openclaw-task-worker:
    image: ${OPENCLAW_IMAGE:-openclaw:local}
    restart: unless-stopped
    environment:
      TEB_REDIS_URL: "redis://redis:6379"
      TEB_QUEUE_PREFIX: "tasks"
      TEB_RESULT_PREFIX: "teb:result"
      TEB_RESULT_TTL: "300"
      TEB_QUEUES: "general,code,qa,research,ui"
      TEB_ALLOW_CODE_RUN: "false" # MUST be set to "true" to enable code-run tasks
      TEB_LOG_LEVEL: "info"
    volumes:
      - "/path/to/task-event-bus/dist:/app/worker/dist:ro"
    command: ["node", "/app/worker/dist/worker.js", "--queues", "general,code,qa,research,ui"]
    depends_on:
      redis:
        condition: service_healthy

> [!WARNING]
> For security, the `code-run` task handler (which executes shell commands) is **disabled by default**. Setting `TEB_ALLOW_CODE_RUN: "true"` exposes the host/container to command execution. Ensure the worker is isolated and appropriate restrictions are configured.
```

### Dynamic Queue Auto-Discovery

The worker dynamically discovers queues as tasks are dispatched. When an agent calls `task_dispatch` to any queue name, the plugin automatically registers that queue in a Redis Set registry (`{queuePrefix}:registry`, e.g., `tasks:registry`). 

The worker periodically queries this registry (every 15 seconds) and dynamically merges the newly registered queues with the baseline list defined by `TEB_QUEUES` (or `--queues` flag). This eliminates the need to update environment variables or restart the worker container when adding new agent queues.


### Built-in task handlers

| Type | Description |
|------|-------------|
| `code-run` | Execute a shell command, capture stdout/stderr. **Disabled by default (requires `TEB_ALLOW_CODE_RUN: "true"` in environment to enable).** |
| `web-fetch` | HTTP GET/POST, return status + body. Supports configurable `timeoutSeconds` field in payload (default: `30` seconds) via AbortController. |
| `agent-run` | Placeholder for sub-agent spawning (register your own) |

### Register a custom handler

```js
// In worker.js
import { registerHandler } from './dist/worker.js';

registerHandler('my-task', async (payload) => {
  // custom logic
  return { success: true, data: payload };
});
```

## Architecture

```
┌──────────────┐     event_publish      ┌──────────────┐
│  Agent A     │──────────────────────▶│              │
└──────────────┘                       │    Redis     │
                                       │  Pub/Sub     │
┌──────────────┐     event_subscribe  │              │
│  Agent B     │◀──────────────────────│              │
└──────────────┘                       └──────────────┘

┌──────────────┐    task_dispatch     ┌──────────────┐
│  Agent       │──────────────────────▶│   Redis      │
│              │                       │   List       │
└──────────────┘                       │  (queue)     │
                                       │              │
                                       │  brpop       │
                                       ▼              │
                               ┌──────────────┐      │
                               │   Worker     │◀─────┘
                               │  Container   │
                               └──────────────┘
                                       │
                              writeResult │ teb:result:{id}
                                       ▼
                               ┌──────────────┐
                               │   Redis      │
                               │   Key        │
                               └──────────────┘
```

## License

MIT
