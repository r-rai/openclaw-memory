# OpenClaw Memory & Task Infrastructure

A production-ready OpenClaw plugin suite providing:

- **MultiTierMemory** — Three-tier memory system: Redis (hot) + Qdrant (cold vector) + llama.cpp embeddings
- **TaskEventBus** — Redis-backed unified task queue + event bus with a persistent worker process

Built for multi-agent OpenClaw deployments where agents need shared memory and asynchronous task processing.

---

## Plugins

### MultiTierMemory

Three-tier memory architecture:

```
Query → [1] Redis (hot, sub-ms)
         └→ [2] Qdrant (cold, semantic search)
         └→ [3] llama.cpp embeddings (nomic-embed-text-v1.5, 768-dim)
```

- `memory_search` — Semantic search across hot + cold tiers
- `memory_get` — Read a specific memory file by path
- `memory_store` — Store a memory entry (auto-caches important items to both tiers)
- `memory_cache_update` — Update the hot/cold cache TTL

[→ Read more](packages/multi-tier-memory/README.md)

---

### TaskEventBus

Redis-backed task queue + event pub/sub for OpenClaw agents:

```
Agent ──publish──→ Redis Pub/Sub ──→ Other agents
Agent ──dispatch──→ Redis List (queue) ──→ Worker container
Worker ──result──→ Redis Key (teb:result:{id})
```

- `event_publish` / `event_subscribe` — Inter-agent pub/sub events
- `task_dispatch` / `task_results` — Async task queue with correlation IDs
- `openclaw-task-worker` — Standalone Docker container consuming the queue

[→ Read more](packages/task-event-bus/README.md)

---

## Quick Start

### 1. Install

```bash
# Clone the repo
git clone https://github.com/r-rai/openclaw-memory.git
cd openclaw-memory

# Install dependencies (pnpm recommended)
pnpm install
```

### 2. Configure OpenClaw

Add to your `openclaw.json`:

```json
{
  "plugins": {
    "entries": {
      "multi-tier-memory": "@openclaw-memory/multi-tier-memory",
      "task-event-bus": "@openclaw-memory/task-event-bus"
    },
    "slots": {
      "memory": "multi-tier-memory"
    },
    "load": {
      "paths": [
        "/path/to/openclaw-memory/packages/multi-tier-memory",
        "/path/to/openclaw-memory/packages/task-event-bus"
      ]
    }
  }
}
```

### 3. Docker Compose Services

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --maxmemory 512mb --maxmemory-policy allkeys-lru
    volumes: ["~/.openclaw/redis_data:/data"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s; timeout: 5s; retries: 3

  qdrant:
    image: qdrant/qdrant:latest
    ports: ["6333:6333"]
    volumes: ["~/.openclaw/qdrant_storage:/qdrant/storage"]

  llama-cpp:
    image: ghcr.io/ggml-org/llama.cpp:server
    ports: ["8080:8080"]
    command: -m /models/nomic-embed-text-v1.5.Q4_K_M.gguf --host 0.0.0.0 --port 8080 --embedding
    volumes: ["/path/to/models:/models"]

  openclaw-task-worker:
    image: ${OPENCLAW_IMAGE:-openclaw:local}
    restart: unless-stopped
    environment:
      TEB_REDIS_URL: "redis://redis:6379"
      TEB_QUEUE_PREFIX: "tasks"
      TEB_RESULT_TTL: "300"
      TEB_QUEUES: "general,code,qa,research,ui"
      TEB_ALLOW_CODE_RUN: "false" # Set to "true" to enable the 'code-run' task type
    volumes:
      - "/path/to/openclaw-memory/packages/task-event-bus/dist:/app/worker/dist:ro"
    command: ["node", "/app/worker/dist/worker.js", "--queues", "general,code,qa,research,ui"]
    depends_on:
      redis:
        condition: service_healthy
```

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for a deep-dive into system design, data flows, and scaling considerations.

---

## License

MIT © Ravi Rai
