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
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

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

    /** @type {import('redis').RedisClientType | null} */
    this._redis = null;
    /** @type {QdrantClient | null} */
    this._qdrant = null;
    this._embedUrl = `${this.embeddingUrl}/embeddings`;
    this._coldInitialized = false;
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
    return redis.get(`mtm:hot:${agentId}:${key}`);
  }

  async _hotSet(agentId, key, value, ttlSeconds) {
    const redis = await this._getRedis();
    await redis.setEx(`mtm:hot:${agentId}:${key}`, ttlSeconds, value);
  }

  async _hotDel(agentId, key) {
    const redis = await this._getRedis();
    await redis.del(`mtm:hot:${agentId}:${key}`);
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
    if (this._coldInitialized) return;
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
    this._coldInitialized = true;
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

  async _coldUpsert(agentId, key, content, embedding, importance, metadata) {
    await this._ensureColdCollection();
    const q = await this._getQdrant();
    const id = `${agentId}:${key}`;
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
      if (agentId) {
        const hotKey = `mtm:hot:${agentId}:*`;
        // Scan for hot entries matching query terms
        const keys = await redis.keys(hotKey);
        for (const k of keys.slice(0, 20)) {
          const v = await redis.get(k);
          if (v) {
            try {
              const item = JSON.parse(v);
              // Light text match against query
              if (item.content && query.toLowerCase().split(' ').some((w) => item.content.toLowerCase().includes(w))) {
                hotItems.push({ source: 'hot', score: 0.95, ...item });
              }
            } catch {
              // not JSON, skip
            }
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
      const results = await q.search(this.coldCollection, {
        vector: new Array(this.vectorSize).fill(0), // dummy — we'll filter by path
        limit: 1,
        filter,
        with_payload: true,
      });

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

  async memory_store({ path, content, importance = 0.5, agentId = 'default', metadata = {} } = {}) {
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
      const key = `mtm:hot:${agentId}:${path}`;
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

// openclaw is a peer dependency. In the OpenClaw container it resolves from
// /app/node_modules/openclaw. For local dev / npm installs, it comes from
// the openclaw package in node_modules.
import { defineToolPlugin } from 'openclaw';

export default defineToolPlugin({
  name: 'multi-tier-memory',
  version: '1.0.0',
  config: ConfigSchema,

  tools: [
    { name: 'memory_search', description: 'Semantic memory search across hot (Redis) and cold (Qdrant) tiers', schema: MemorySearchSchema, handler: (args, ctx) => ctx.plugin.memory_search(args) },
    { name: 'memory_get', description: 'Read a specific memory entry by path', schema: MemoryGetSchema, handler: (args, ctx) => ctx.plugin.memory_get(args) },
    { name: 'memory_store', description: 'Store a memory entry (auto-caches important items to cold tier)', schema: MemoryStoreSchema, handler: (args, ctx) => ctx.plugin.memory_store(args) },
    { name: 'memory_cache_update', description: 'Update the TTL of a hot-cache entry in Redis', schema: MemoryCacheUpdateSchema, handler: (args, ctx) => ctx.plugin.memory_cache_update(args) },
  ],

  factory: (config) => new MultiTierMemoryPlugin(config),
});
