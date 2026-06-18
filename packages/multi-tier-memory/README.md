# MultiTierMemory

Three-tier memory system for OpenClaw agents:
- **HOT** → Redis (sub-ms active context cache)
- **COLD** → Qdrant (semantic vector search, long-term memory)
- **EMBEDDING** → llama.cpp (`nomic-embed-text-v1.5`, 768-dim)

## Tools

| Tool | Description |
|------|-------------|
| `memory_search` | Semantic search across hot + cold tiers |
| `memory_get` | Read a specific memory entry by path |
| `memory_store` | Store a memory entry (auto-caches `importance ≥ 0.7` to cold tier). **Note: `agentId` is required.** |
| `memory_cache_update` | Update the TTL of a hot-cache Redis entry |

> [!IMPORTANT]
> The `agentId` parameter is **required** for `memory_store` and `memory_search` operations. It no longer defaults to `'default'`.

## Installation

```bash
pnpm add @openclaw-memory/multi-tier-memory
```

## OpenClaw Configuration

```json
{
  "plugins": {
    "entries": {
      "multi-tier-memory": "/path/to/node_modules/@openclaw-memory/multi-tier-memory"
    },
    "slots": {
      "memory": "multi-tier-memory"
    },
    "load": {
      "paths": ["/path/to/node_modules/@openclaw-memory/multi-tier-memory"]
    }
  }
}
```

Or in `openclaw.json` plugin config:

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

### Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `redisUrl` | `redis://redis:6379` | Redis connection URL |
| `qdrantUrl` | `http://qdrant:6333` | Qdrant server URL |
| `embeddingUrl` | `http://llama-cpp:8080/v1` | llama.cpp embed API endpoint |
| `embeddingModel` | `nomic-embed-text-v1.5.Q4_K_M.gguf` | Embedding model name |
| `vectorSize` | `768` | Dimension size of vector embeddings |
| `hotTtlSeconds` | `3600` | TTL (in seconds) for hot memory cache in Redis |
| `coldCollection` | `agent_memory` | Name of the collection in Qdrant |
| `hotKeyPrefix` | `mtm:hot` | Key prefix used for Redis hot cache entries |

## Architecture

```
Agent query
    │
    ▼
┌─────────────┐    no hit    ┌────────────────┐
│   Redis     │─────────────▶│    Qdrant      │
│   (hot)     │◀──────────────│   (cold)       │
└─────────────┘   recall     └────────────────┘
     │                        │
     │                        │
     └────────┬────────────────┘
              │ embedding
              ▼
     ┌─────────────────┐
     │   llama.cpp     │
     │ (nomic-embed)   │
     └─────────────────┘
```

### Hot tier (Redis)
- Key pattern: `{hotKeyPrefix}:{agentId}:{path}` (default prefix: `mtm:hot`)
- TTL: configurable (default 3600s)
- Use for: recent conversation context, frequently accessed memories

### Cold tier (Qdrant)
- Collection: `agent_memory` (auto-created)
- 768-dim cosine similarity search
- Auto-stores entries with `importance ≥ 0.7`
- Use for: long-term semantic recall

## Development

```bash
# Build
pnpm build

# The dist/ directory is committed so users can run without building
```

## License

MIT
