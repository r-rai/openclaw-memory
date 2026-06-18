/**
 * MultiTierMemory — OpenClaw memory plugin
 *
 * Three-tier architecture:
 *  - HOT  → Redis        (active context cache, sub-ms reads)
 *  - COLD → Qdrant       (semantic vector search, long-term memory)
 *  - embedding → llama.cpp (nomic-embed-text-v1.5, 768-dim)
 *
 * Supplies: memory_search, memory_get, memory_store, memory_cache_update
 */

import { createRequire } from 'module';
import crypto from 'crypto';

// ---------------------------------------------------------------------------
// Resolve CJS packages relative to this file's location
// (works in both local dev and inside OpenClaw container node_modules)
// ---------------------------------------------------------------------------
const _require = createRequire(import.meta.url);

const { createClient: _createRedisClient } = _require('redis');
const { QdrantClient } = _require('@qdrant/js-client-rest');

// ---------------------------------------------------------------------------
// Config — keys map to openclaw.json plugin config
// ---------------------------------------------------------------------------

const ConfigSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    redisUrl: {
      type: 'string',
      default: 'redis://redis:6379',
      description: 'Redis connection URL (hot cache + pub/sub)',
    },
    qdrantUrl: {
      type: 'string',
      default: 'http://qdrant:6333',
      description: 'Qdrant HTTP URL (cold vector storage)',
    },
    embeddingUrl: {
      type: 'string',
      default: 'http://llama-cpp:8080/v1',
      description: 'llama.cpp embeddings API base URL',
    },
    embeddingModel: {
      type: 'string',
      default: 'nomic-embed-text-v1.5.Q4_K_M.gguf',
      description: 'Model name passed to /v1/embeddings',
    },
    vectorSize: {
      type: 'integer',
      default: 768,
      description: 'Embedding dimensionality (must match your model)',
    },
    hotTtlSeconds: {
      type: 'integer',
      default: 3600,
      description: 'How long hot-cache entries live in Redis (seconds)',
    },
    coldCollection: {
      type: 'string',
      default: 'agent_memory',
      description: 'Qdrant collection name for cold-tier vectors',
    },
    hotKeyPrefix: {
      type: 'string',
      default: 'mtm:hot',
      description: 'Redis key prefix for hot-cache entries (change to avoid collisions on shared Redis)',
    },
  },
};

// ---------------------------------------------------------------------------
// Tool schemas (TypeBox-compatible plain objects)
// ---------------------------------------------------------------------------

const MemorySearchSchema = {
  type: 'object',
  properties: {
    query: {
      type: 'string',
      description: 'Natural-language search query — describe what you want to recall',
    },
    maxResults: {
      type: 'integer',
      minimum: 1,
      default: 10,
      description: 'Maximum number of results to return',
    },
    minScore: {
      type: 'number',
      minimum: 0,
      maximum: 1,
      description: 'Minimum cosine-similarity score threshold',
    },
    agentId: {
      type: 'string',
      description: 'Agent ID to scope the search (filters to that agent\'s memories)',
    },
  },
  required: ['query'],
  additionalProperties: false,
};

const MemoryGetSchema = {
  type: 'object',
  properties: {
    path: {
      type: 'string',
      description: 'Memory file path to read (e.g. MEMORY.md, memory/2026-06-14.md)',
    },
    from: {
      type: 'integer',
      minimum: 1,
      description: 'Start reading from this line number (1-indexed)',
    },
    lines: {
      type: 'integer',
      minimum: 1,
      description: 'Maximum number of lines to read',
    },
    agentId: {
      type: 'string',
      description: 'Agent ID to scope the read (required for agent-specific memories)',
    },
  },
  required: ['path'],
  additionalProperties: false,
};

const MemoryStoreSchema = {
  type: 'object',
  properties: {
    path: {
      type: 'string',
      description: 'Path/key for this memory entry (e.g. memory/2026-06-14.md)',
    },
    content: {
      type: 'string',
      description: 'Content to store',
    },
    importance: {
      type: 'number',
      minimum: 0,
      maximum: 1,
      default: 0.5,
      description: 'Importance score; entries >= 0.7 are auto-cached to cold tier',
    },
    agentId: {
      type: 'string',
      description: 'Agent ID this memory belongs to',
    },
    metadata: {
      type: 'object',
      description: 'Arbitrary metadata to attach (tags, timestamps, etc.)',
    },
  },
  required: ['path', 'content'],
  additionalProperties: false,
};

const MemoryCacheUpdateSchema = {
  type: 'object',
  properties: {
    path: {
      type: 'string',
      description: 'Memory path/key to update the cache TTL for',
    },
    hotTtlSeconds: {
      type: 'integer',
      minimum: 1,
      description: 'New TTL for the Redis hot-cache entry',
    },
    agentId: {
      type: 'string',
      description: 'Agent ID',
    },
  },
  required: ['path'],
  additionalProperties: false,
};

// ---------------------------------------------------------------------------
// Plugin class
// ---------------------------------------------------------------------------

class MultiTierMemoryPlugin {
  constructor(config) {
    this.redisUrl = config.redisUrl ?? 'redis://redis:6379';
    this.qdrantUrl = config.qdrantUrl ?? 'http://qdrant:6333';
    this.embeddingUrl = config.embeddingUrl ?? 'http://llama-cpp:8080/v1';
    this.embeddingModel = config.embeddingModel ?? 'nomic-embed-text-v1.5.Q4_K_M.gguf';
    this.vectorSize = config.vectorSize ?? 768;
    this.hotTtlSeconds = config.hotTtlSeconds ?? 3600;
    this.coldCollection = config.coldCollection ?? 'agent_memory';
    this.hotKeyPrefix = config.hotKeyPrefix ?? 'mtm:hot';

    /** @type {import('redis').RedisClientType | null} */
    this._redis = null;
    /** @type {QdrantClient | null} */
    this._qdrant = null;
    this._embedUrl = `${this.embeddingUrl}/embeddings`;
    /** @type {Promise<void> | null} — promise lock to prevent concurrent collection creation */
    this._coldInitPromise = null;
  }

  // ── Redis ──────────────────────────────────────────────────────────────

  async _getRedis() {
    if (!this._redis) {
      this._redis = _createRedisClient({ url: this.redisUrl });
      this._redis.on('error', (err) => console.error('[MTM Redis]', err.message));
      await this._redis.connect();
    }
    return this._redis;
  }

  async _hotGet(agentId, key) {
    const redis = await this._getRedis();
    return redis.get(`${this.hotKeyPrefix}:${agentId}:${key}`);
  }

  async _hotSet(agentId, key, value, ttlSeconds) {
    const redis = await this._getRedis();
    await redis.setEx(`${this.hotKeyPrefix}:${agentId}:${key}`, ttlSeconds, value);
  }

  async _hotDel(agentId, key) {
    const redis = await this._getRedis();
    await redis.del(`${this.hotKeyPrefix}:${agentId}:${key}`);
  }

  // ── Embeddings ─────────────────────────────────────────────────────────

  async _embed(text) {
    const res = await fetch(this._embedUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: text, model: this.embeddingModel }),
    });
    if (!res.ok) throw new Error(`Embedding API error: ${res.status} ${res.statusText}`);
    const data = await res.json();
    return data.data?.[0]?.embedding ?? null;
  }

  // ── Qdrant ─────────────────────────────────────────────────────────────

  async _getQdrant() {
    if (!this._qdrant) {
      this._qdrant = new QdrantClient({ url: this.qdrantUrl });
    }
    return this._qdrant;
  }

  async _ensureColdCollection() {
    // Promise-based lock: all concurrent callers await the same init promise
    if (!this._coldInitPromise) {
      this._coldInitPromise = (async () => {
        const q = await this._getQdrant();
        try {
          await q.getCollection(this.coldCollection);
        } catch {
          // Collection doesn't exist — create it
          await q.createCollection(this.coldCollection, {
            vectors: { size: this.vectorSize, distance: 'Cosine' },
          });
          console.log(`[MTM] Created Qdrant collection: ${this.coldCollection}`);
        }
      })();
    }
    return this._coldInitPromise;
  }

  async _coldSearch(embedding, limit, minScore, agentId) {
    await this._ensureColdCollection();
    const q = await this._getQdrant();
    const filter = agentId ? { must: [{ key: 'agentId', match: { value: agentId } }] } : undefined;
    const results = await q.search(this.coldCollection, {
      vector: embedding,
      limit,
      score_threshold: minScore,
      filter,
      with_payload: true,
    });
    return results.map((r) => ({
      id: r.id,
      score: r.score,
      path: r.payload?.path,
      content: r.payload?.content,
      importance: r.payload?.importance,
      agentId: r.payload?.agentId,
      metadata: r.payload?.metadata,
    }));
  }

  _generateDeterministicUuid(agentId, path) {
    const hash = crypto.createHash('md5').update(`${agentId}:${path}`).digest('hex');
    return `${hash.slice(0, 8)}-${hash.slice(8, 12)}-${hash.slice(12, 16)}-${hash.slice(16, 20)}-${hash.slice(20, 32)}`;
  }

  async _coldUpsert(agentId, key, content, embedding, importance, metadata) {
    await this._ensureColdCollection();
    const q = await this._getQdrant();
    const id = this._generateDeterministicUuid(agentId, key);
    await q.upsert(this.coldCollection, {
      wait: true,
      points: [
        {
          id,
          vector: embedding,
          payload: { path: key, content, importance, agentId, metadata, storedAt: new Date().toISOString() },
        },
      ],
    });
  }

  // ── Tool implementations ─────────────────────────────────────────────────

  async memory_search({ query, maxResults = 10, minScore = 0.0, agentId } = {}) {
    try {
      // Get query embedding
      const embedding = await this._embed(query);
      if (!embedding) throw new Error('Failed to generate embedding for query');

      // Search cold tier (Qdrant) — primary semantic search
      const coldResults = await this._coldSearch(embedding, maxResults, minScore, agentId);

      // Also check hot tier (Redis) for recent entries
      const redis = await this._getRedis();
      let hotItems = [];
      const prefixPattern = `${this.hotKeyPrefix}:`;
      const scanPattern = agentId ? `${this.hotKeyPrefix}:${agentId}:*` : `${this.hotKeyPrefix}:*`;

      // Scan for matching hot keys
      const keys = [];
      for await (const key of redis.scanIterator({ MATCH: scanPattern, COUNT: 100 })) {
        keys.push(key);
        if (keys.length >= 20) break;
      }

      // Batch fetch all values in a single round-trip
      if (keys.length > 0) {
        const values = await redis.mGet(keys);
        for (let i = 0; i < keys.length; i++) {
          const v = values[i];
          if (!v) continue;
          try {
            const item = JSON.parse(v);
            // Light text match against query
            if (item.content && query.toLowerCase().split(' ').some((w) => item.content.toLowerCase().includes(w))) {
              const parts = keys[i].substring(prefixPattern.length).split(':');
              const itemAgentId = parts[0];
              const path = parts.slice(1).join(':');
              hotItems.push({ source: 'hot', score: 0.95, path, agentId: itemAgentId, ...item });
            }
          } catch {
            // not JSON, skip
          }
        }
      }

      // Deduplicate: prefer cold results, add hot items not already present
      const coldPaths = new Set(coldResults.map((r) => r.path));
      const uniqueHot = hotItems.filter((h) => !coldPaths.has(h.path));

      return {
        query,
        hotCount: uniqueHot.length,
        coldCount: coldResults.length,
        results: [...uniqueHot, ...coldResults].slice(0, maxResults),
      };
    } catch (err) {
      return { error: err.message, query, results: [] };
    }
  }

  async memory_get({ path, from, lines, agentId } = {}) {
    try {
      // Check hot cache first
      if (agentId) {
        const cached = await this._hotGet(agentId, path);
        if (cached) {
          const item = JSON.parse(cached);
          return { path, source: 'hot', content: item.content, metadata: item.metadata };
        }
      }

      // Fall back to cold search for this specific path
      await this._ensureColdCollection();
      const q = await this._getQdrant();
      const filter = {
        must: [
          { key: 'path', match: { value: path } },
          ...(agentId ? [{ key: 'agentId', match: { value: agentId } }] : []),
        ],
      };
      
      const response = await q.scroll(this.coldCollection, {
        filter,
        limit: 1,
        with_payload: true,
        with_vector: false
      });
      const results = response.points || [];

      if (!results.length) return { error: 'Memory not found', path };

      const hit = results[0].payload;
      let content = hit.content ?? '';

      // Apply line range if specified
      if (from || lines) {
        const allLines = content.split('\n');
        const start = Math.max(0, (from ?? 1) - 1);
        const end = lines ? start + lines : allLines.length;
        content = allLines.slice(start, end).join('\n');
      }

      return { path, source: 'cold', content, importance: hit.importance, metadata: hit.metadata };
    } catch (err) {
      return { error: err.message, path };
    }
  }

  async memory_store({ path, content, importance = 0.5, agentId, metadata = {} } = {}) {
    if (!agentId) return { error: 'agentId is required for memory_store' };
    try {
      // Always write to hot cache
      await this._hotSet(agentId, path, JSON.stringify({ content, importance, metadata }), this.hotTtlSeconds);

      // Auto-cache important items to cold tier too
      if (importance >= 0.7) {
        const embedding = await this._embed(content);
        if (embedding) {
          await this._coldUpsert(agentId, path, content, embedding, importance, metadata);
        }
      }

      return { path, stored: true, hotCached: true, coldCached: importance >= 0.7 };
    } catch (err) {
      return { error: err.message, path };
    }
  }

  async memory_cache_update({ path, hotTtlSeconds, agentId } = {}) {
    try {
      if (!agentId) return { error: 'agentId is required' };

      const redis = await this._getRedis();
      const key = `${this.hotKeyPrefix}:${agentId}:${path}`;
      const existing = await redis.get(key);

      if (!existing) return { path, updated: false, error: 'Hot cache entry not found' };

      const ttl = hotTtlSeconds ?? this.hotTtlSeconds;
      await redis.setEx(key, ttl, existing);

      return { path, updated: true, hotTtlSeconds: ttl };
    } catch (err) {
      return { error: err.message, path };
    }
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────

  async initialize() {
    // Warm connections
    await Promise.all([this._getRedis(), this._getQdrant()]);
    await this._ensureColdCollection();
    console.log('[MultiTierMemory] Initialized');
  }

  async destroy() {
    if (this._redis) {
      await this._redis.quit();
      this._redis = null;
    }
  }
}

// ---------------------------------------------------------------------------
// Plugin registration
// ---------------------------------------------------------------------------

let _plugin = null;
function getPlugin(config) {
  if (!_plugin) {
    _plugin = new MultiTierMemoryPlugin(config);
    _plugin.initialize().catch((err) => console.error('[MTM Initialize Error]', err));
  } else {
    // Config is frozen at first init — restart OpenClaw to apply config changes
    console.warn('[MTM] Config update ignored — plugin already initialized. Restart to apply changes.');
  }
  return _plugin;
}

import { defineToolPlugin } from 'openclaw/plugin-sdk/tool-plugin';

export default defineToolPlugin({
  id: 'multi-tier-memory',
  name: 'Multi-Tier Memory',
  description: 'Three-tier memory system: Redis (hot) + Qdrant (cold) + llama.cpp embeddings',
  configSchema: ConfigSchema,
  tools: (tool) => [
    tool({
      name: 'memory_search',
      label: 'Memory Search',
      description: 'Semantic memory search across hot (Redis) and cold (Qdrant) tiers',
      parameters: MemorySearchSchema,
      execute(params, config, context) {
        return getPlugin(config).memory_search(params);
      }
    }),
    tool({
      name: 'memory_get',
      label: 'Memory Get',
      description: 'Read a specific memory entry by path',
      parameters: MemoryGetSchema,
      execute(params, config, context) {
        return getPlugin(config).memory_get(params);
      }
    }),
    tool({
      name: 'memory_store',
      label: 'Memory Store',
      description: 'Store a memory entry (auto-caches important items to cold tier)',
      parameters: MemoryStoreSchema,
      execute(params, config, context) {
        return getPlugin(config).memory_store(params);
      }
    }),
    tool({
      name: 'memory_cache_update',
      label: 'Memory Cache Update',
      description: 'Update the TTL of a hot-cache entry in Redis',
      parameters: MemoryCacheUpdateSchema,
      execute(params, config, context) {
        return getPlugin(config).memory_cache_update(params);
      }
    }),
  ]
});
