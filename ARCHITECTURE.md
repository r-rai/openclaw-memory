# Architecture — OpenClaw Memory & Task Infrastructure

## Overview

This project provides two OpenClaw plugins that extend the gateway with production-grade memory and task infrastructure:

1. **MultiTierMemory** — Three-tier memory spanning Redis (hot), Qdrant (cold), and llama.cpp embeddings
2. **TaskEventBus** — Redis-backed event bus + async task queue with a persistent worker

Both plugins are designed for Docker-based deployments where OpenClaw runs inside a container and infrastructure services (Redis, Qdrant, llama.cpp) run as separate containers on the same Docker bridge network.

---

## MultiTierMemory

### Data Flow

```
Agent asks "what was I working on yesterday?"
         │
         ▼
┌─────────────────────────────────────────────────────┐
│ memory_search tool                                  │
│                                                     │
│  1. Encode query with llama.cpp embeddings API      │
│     POST /v1/embeddings                             │
│                                                     │
│  2. Search cold tier (Qdrant)                       │
│     Cosine similarity search on agent_memory        │
│     collection, filtered by agentId                 │
│                                                     │
│  3. Also check hot tier (Redis)                    │
│     Keys: mtm:hot:{agentId}:*                       │
│     Light text-match against query tokens           │
│                                                     │
│  4. Deduplicate + merge results                     │
│     Hot results get score 0.95 (high confidence)    │
│     Cold results ranked by cosine similarity       │
└─────────────────────────────────────────────────────┘
         │
         ▼
  Final results returned to agent
```

### Why Three Tiers?

| Tier | Store | Latency | Use Case |
|------|-------|---------|----------|
| **Hot** | Redis | < 1ms | Recent context, frequently accessed memories |
| **Cold** | Qdrant | 5–20ms | Semantic long-term recall across all history |
| **Embed** | llama.cpp | 10–50ms | Text → vector conversion |

The hot tier catches recent activity without hitting Qdrant on every query. Only items with `importance ≥ 0.7` are promoted to the cold tier, keeping Qdrant lean.

### Redis Key Patterns

```
mtm:hot:{agentId}:{path}   → JSON string, TTL = hotTtlSeconds
```

### Qdrant Collection

```
Collection:  agent_memory
Vector:     768-dim, Cosine distance
Payload:    path, content, importance, agentId, metadata, storedAt
Filter:     agentId (optional scope)
```

Qdrant collection is auto-created on first `memory_store` call.

---

## TaskEventBus

### Event Bus (Pub/Sub)

```
Agent A                          Agent B
  │                                │
  │── publish(channel, payload) ──▶│
  │    teb:events:agent.ui         │
  │                                │
  │◀── subscribe(channel) ─────────│
```

- Channels use `:` separator convention: `events:task.*`, `events:agent.ui`, `events:system:*`
- Subscriber connection is separate from publisher connection (Redis requirement)
- Subscribe is one-shot with configurable `timeoutMs` — not a persistent listener

### Task Queue (Lists)

```
Agent                              Worker
  │                                   │
  │── lpush tasks:code, {task} ──────▶│
  │                                   │
  │                                   │── brpop tasks:code (blocking)
  │                                   │
  │                                   │── process task
  │                                   │
  │◀── get teb:result:{id} ───────────│
  │    (poll until status != pending) │
```

- Tasks are JSON objects with `correlationId`, `queue`, `payload.type`
- Standard tasks: FIFO via `lpush` + `brpop`
- Priority tasks (1–10): sorted set via `zadd` + `zpopmin`
- Results stored in Redis with TTL: `teb:result:{correlationId}`

### Idempotency

Before dispatching, the plugin checks if `teb:result:{correlationId}` already exists. If it does, the task is skipped and the current status is returned — safe to call multiple times.

### Worker Lifecycle

```
Worker starts
    │
    ▼
Connect to Redis (dedicated pub + sub connections)
    │
    ▼
┌─────────────────────────────────────┐
│ Loop (infinite):                   │
│                                     │
│  for each queue in CONFIG.queues:  │
│    task = brpop(queue, timeout=3)   │
│    if task:                         │
│      mark processing               │
│      handler = handlers[task.type] │
│      result = await handler()       │
│      writeResult(correlationId)    │
│      continue                       │
│                                     │
│  if no task from any queue:        │
│    sleep POLL_INTERVAL             │
└─────────────────────────────────────┘
```

### Built-in Handlers

| Type | Description |
|------|-------------|
| `code-run` | `child_process.spawn('/bin/sh', ['-c', command])` — returns stdout, stderr, exit code |
| `web-fetch` | `fetch()` — returns status, headers, body (capped at 10k chars) |
| `agent-run` | Placeholder — register your own with `registerHandler()` |

### Scaling Workers

Each worker has a unique auto-generated ID: `worker-{hostname}-{timestamp}-{random}`. To scale:

```bash
docker compose up -d --scale openclaw-task-worker=3 openclaw-task-worker
```

Since workers use `brpop` (blocking pop), Redis atomically distributes work — each task goes to exactly one worker.

### Failure Modes

| Scenario | Behavior |
|----------|----------|
| Worker crashes mid-task | Result stays `status: processing` — caller times out |
| Redis goes down | Worker reconnects automatically on next `brpop` |
| Task handler throws | Result written as `status: failed`, error message stored |
| Task times out | Handler sends `SIGKILL` after `timeoutSeconds` |
| Web fetch times out | Request aborted via AbortController after `timeoutSeconds` (default: 30s), returning error: 'TIMEOUT' |

---

## Networking

All services run on the same Docker bridge network (`openclaw_default`):

```
redis://redis:6379        → Redis container
http://qdrant:6333        → Qdrant container
http://llama-cpp:8080/v1  → llama.cpp server container
```

OpenClaw gateway and the task worker both resolve these via Docker DNS.

---

## Configuration Reference

### MultiTierMemory (`openclaw.json`)

```json
{
  "multi-tier-memory": {
    "redisUrl": "redis://redis:6379",
    "qdrantUrl": "http://qdrant:6333",
    "embeddingUrl": "http://llama-cpp:8080/v1",
    "embeddingModel": "nomic-embed-text-v1.5.Q4_K_M.gguf",
    "vectorSize": 768,
    "hotTtlSeconds": 3600,
    "coldCollection": "agent_memory",
    "hotKeyPrefix": "mtm:hot"
  }
}
```

### TaskEventBus (`openclaw.json`)

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

### Task Worker (env vars)

| Variable | Default | Description |
|----------|---------|-------------|
| `TEB_REDIS_URL` | `redis://redis:6379` | Redis connection URL |
| `TEB_QUEUE_PREFIX` | `tasks` | Redis key prefix for queues |
| `TEB_RESULT_PREFIX` | `teb:result` | Redis key prefix for results (must match plugin config) |
| `TEB_WORKER_ID` | auto-generated | Unique worker ID |
| `TEB_QUEUES` | `general,code,qa,research,ui` | Queues to consume |
| `TEB_POLL_INTERVAL` | `3000` | Poll interval when queues empty (ms) |
| `TEB_RESULT_TTL` | `300` | Result key TTL (seconds) |
| `TEB_ALLOW_CODE_RUN` | `false` | Set to `true` to enable the 'code-run' task type |
| `TEB_LOG_LEVEL` | `info` | debug \| info \| warn \| error |

---

## Security Considerations

- The worker runs code via `code-run` tasks — for security, this handler is **disabled by default** and requires setting `TEB_ALLOW_CODE_RUN=true` in the environment to enable.
- Redis has no authentication by default — use `requirepass` or network-level isolation
- Qdrant has no authentication by default — bind to internal network only
- `hotTtlSeconds` limits how long unencrypted memory lives in Redis

---

## Performance Notes

- Redis `brpop` is O(1) — queue depth has no impact on pop speed. The worker loop optimizes pops by checking priority queues with non-blocking `zPopMin` first, then utilizing a single blocking multi-key `brPop` call for the lower priority queues to minimize CPU polling overhead.
- Dynamic Queue Auto-Discovery: The plugin dynamically registers queue names in a Redis Set registry (`tasks:registry`) on task dispatch, and the worker periodically (every 15s) queries this registry to update its active queues dynamically without restarts.
- Redis search uses `mGet` to batch text matching queries instead of calling Redis sequentially, reducing round-trips.
- Qdrant collection initialization utilizes a promise-lock to prevent concurrent schema creation race conditions during initialization.
- Qdrant search is O(n) for the collection size — `agent_memory` stays lean by only storing `importance ≥ 0.7` entries
- llama.cpp embeddings are the main latency source (~10–50ms per query)
- Each agent should use a distinct `agentId` to scope memory and avoid cross-contamination
