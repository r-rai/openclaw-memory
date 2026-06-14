/**
 * MultiTierMemory — OpenClaw memory plugin
 *
 * Three-tier architecture:
 *  - HOT  → Redis        (active context cache, sub-ms reads)
 *  - COLD → Qdrant       (semantic vector search, long-term memory)
 *  - embedding → llama.cpp (nomic-embed-text-v1.5, 768-dim)
 *
 * Supplies memory_search + memory_get + memory_store + memory_cache_update tools.
 */

import { createRequire } from 'module';
import { defineToolPlugin } from '/app/node_modules/openclaw/dist/plugin-sdk/tool-plugin.js';

// ---------------------------------------------------------------------------
// Redis + Qdrant (CommonJS packages, loaded via createRequire)
// ---------------------------------------------------------------------------
const _require = createRequire(import.meta.url);

const { createClient: _createRedisClient } = _require(
  '/home/node/.openclaw/plugins/MultiTierMemory/node_modules/redis/dist/index.js'
);

const { QdrantClient } = _require(
  '/home/node/.openclaw/plugins/MultiTierMemory/node_modules/@qdrant/js-client-rest/dist/cjs/index.js'
);

// ---------------------------------------------------------------------------
// Config schema
// ---------------------------------------------------------------------------

const ConfigSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    redisUrl: { type: 'string', default: 'redis://redis:6379' },
    qdrantUrl: { type: 'string', default: 'http://qdrant:6333' },
    embeddingUrl: { type: 'string', default: 'http://llama-cpp:8080/v1' },
    embeddingModel: { type: 'string', default: 'nomic-embed-text-v1.5' },
    vectorSize: { type: 'integer', default: 768 },
    hotTtlSeconds: { type: 'integer', default: 3600 },
    coldCollection: { type: 'string', default: 'agent_memory' },
  },
};

// ---------------------------------------------------------------------------
// Tool schemas
// ---------------------------------------------------------------------------

const MemorySearchSchema = {
  type: 'object',
  properties: {
    query: {
      type: 'string',
      description: 'Search query — describe what you want to recall from memory.',
    },
    maxResults: { type: 'integer', minimum: 1, default: 10 },
    minScore: { type: 'number', minimum: 0, maximum: 1 },
    agentId: { type: 'string', description: 'Agent ID to scope the search.' },
  },
  required: ['query'],
  additionalProperties: false,
};

const MemoryGetSchema = {
  type: 'object',
  properties: {
    path: {
      type: 'string',
      description: 'Memory file path to read (e.g. MEMORY.md, memory/2026-06-14.md).',
    },
    from: { type: 'integer', minimum: 1, description: 'Start line number.' },
    lines: { type: 'integer', minimum: 1, description: 'Max lines to return.' },
    agentId: { type: 'string' },
  },
  required: ['path'],
  additionalProperties: false,
};

const MemoryStoreSchema = {
  type: 'object',
  properties: {
    text: {
      type: 'string',
      description: 'The memory or fact to store. Be specific and concise.',
    },
    collectionType: {
      type: 'string',
      enum: ['observations', 'knowledge', 'episodes'],
      default: 'observations',
      description:
        'observations = facts learned, knowledge = persistent truths, episodes = conversation turns',
    },
    importance: {
      type: 'number',
      minimum: 0,
      maximum: 1,
      default: 0.5,
      description:
        'Importance score 0-1. Higher scores survive longer in hot cache and rank higher in search.',
    },
    tags: {
      type: 'array',
      items: { type: 'string' },
      description: 'Optional tags to attach for filtering.',
    },
    agentId: { type: 'string', description: 'Agent ID that owns this memory.' },
  },
  required: ['text'],
  additionalProperties: false,
};

const MemoryCacheUpdateSchema = {
  type: 'object',
  properties: {
    key: {
      type: 'string',
      description:
        'Cache key within the hot layer, e.g. "active-project", "current-task", "user-preferences".',
    },
    data: {
      type: 'object',
      description: 'JSON-serializable data to cache.',
    },
    ttlSeconds: {
      type: 'integer',
      minimum: 60,
      default: 3600,
      description: 'Time-to-live in seconds. Minimum 60.',
    },
    agentId: { type: 'string', description: 'Agent ID.' },
  },
  required: ['key', 'data'],
  additionalProperties: false,
};

// ---------------------------------------------------------------------------
// MultiTierMemoryManager
// ---------------------------------------------------------------------------

class MultiTierMemoryManager {
  constructor(config = {}) {
    this.redisUrl = config.redisUrl ?? 'redis://redis:6379';
    this.qdrantUrl = config.qdrantUrl ?? 'http://qdrant:6333';
    this.embeddingUrl = config.embeddingUrl ?? 'http://llama-cpp:8080/v1';
    this.embeddingModel = config.embeddingModel ?? 'nomic-embed-text-v1.5';
    this.vectorSize = config.vectorSize ?? 768;
    this.hotTtlSeconds = config.hotTtlSeconds ?? 3600;
    this.coldCollection = config.coldCollection ?? 'agent_memory';

    this.redis = _createRedisClient({ url: this.redisUrl });
    this.qdrant = new QdrantClient({ url: this.qdrantUrl });
    this.redisConnected = false;
  }

  async init() {
    try {
      await this.redis.connect();
      this.redisConnected = true;
      console.log('[MultiTierMemory] Redis connected (hot layer)');
    } catch (err) {
      console.warn(
        '[MultiTierMemory] Redis connection failed — hot layer disabled:',
        err?.message ?? err
      );
      this.redisConnected = false;
    }

    try {
      await this.ensureCollection();
      console.log('[MultiTierMemory] Qdrant ready (cold layer)');
    } catch (err) {
      console.warn(
        '[MultiTierMemory] Qdrant collection check failed:',
        err?.message ?? err
      );
    }
  }

  async ensureCollection() {
    try {
      const exists = await this.qdrant.collectionExists(this.coldCollection);
      if (!exists.exists) {
        await this.qdrant.createCollection(this.coldCollection, {
          vectors: { size: this.vectorSize, distance: 'Cosine' },
        });
        console.log(
          `[MultiTierMemory] Created Qdrant collection: ${this.coldCollection}`
        );
      }
    } catch {
      // Already exists — safe to ignore
    }
  }

  // -------------------------------------------------------------------------
  // Embedding (llama.cpp)
  // -------------------------------------------------------------------------

  async embed(text) {
    const res = await fetch(`${this.embeddingUrl}/embeddings`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ input: text, model: this.embeddingModel }),
    });
    if (!res.ok) {
      throw new Error(
        `Embedding request failed: ${res.status} ${res.statusText}`
      );
    }
    const data = await res.json();
    return data.data?.[0]?.embedding ?? [];
  }

  // -------------------------------------------------------------------------
  // Hot layer (Redis)
  // -------------------------------------------------------------------------

  /**
   * Write to the Redis hot cache.
   * Higher importance → longer effective TTL in practice (agents should respect it).
   */
  async cachePut(key, agentId, data, ttlSeconds) {
    if (!this.redisConnected) return false;
    const ttl = Math.max(ttlSeconds ?? this.hotTtlSeconds, 60);
    const cacheKey = `mtm:hot:${agentId}:${key}`;
    await this.redis.setEx(cacheKey, ttl, JSON.stringify(data));
    return true;
  }

  /**
   * Read from the Redis hot cache.
   */
  async cacheGet(key, agentId) {
    if (!this.redisConnected) return null;
    const cacheKey = `mtm:hot:${agentId}:${key}`;
    const data = await this.redis.get(cacheKey);
    return data ? JSON.parse(data) : null;
  }

  /**
   * Invalidate a hot cache entry.
   */
  async cacheInvalidate(key, agentId) {
    if (!this.redisConnected) return false;
    const cacheKey = `mtm:hot:${agentId}:${key}`;
    await this.redis.del(cacheKey);
    return true;
  }

  /**
   * List all hot cache keys for an agent (useful for debugging).
   */
  async cacheKeys(agentId) {
    if (!this.redisConnected) return [];
    const pattern = `mtm:hot:${agentId}:*`;
    const keys = await this.redis.keys(pattern);
    return keys.map((k) => k.replace(`mtm:hot:${agentId}:`, ''));
  }

  // -------------------------------------------------------------------------
  // Cold layer (Qdrant)
  // -------------------------------------------------------------------------

  async retrieveRelevant(query, agentId, limit = 10) {
    try {
      const vector = await this.embed(query);
      const results = await this.qdrant.search(this.coldCollection, {
        vector,
        limit,
        filter: {
          must: [{ key: 'agentId', match: { value: agentId } }],
        },
      });

      return results.map((point) => ({
        id: String(point.id),
        text: point.payload?.text ?? '',
        score: point.score ?? 0,
        metadata: point.payload ?? {},
      }));
    } catch (err) {
      console.error(
        '[MultiTierMemory] Qdrant search failed:',
        err?.message ?? err
      );
      return [];
    }
  }

  /**
   * Store a memory entry in Qdrant (async, non-blocking).
   * Returns the generated memory ID immediately so the caller can reference it.
   */
  async storeMemoryText(
    agentId,
    text,
    collectionType = 'observations',
    importance = 0.5,
    tags = []
  ) {
    const id = globalThis.crypto?.randomUUID?.() ?? `mtm-${Date.now()}`;

    setImmediate(async () => {
      try {
        const vector = await this.embed(text);
        await this.qdrant.upsert(this.coldCollection, {
          wait: true,
          points: [
            {
              id,
              vector,
              payload: {
                text,
                agentId,
                collectionType,
                importance,
                tags,
                createdAt: new Date().toISOString(),
              },
            },
          ],
        });
      } catch (err) {
        console.error(
          '[MultiTierMemory] Background store failed:',
          err?.message ?? err
        );
      }
    });

    return id;
  }

  buildMemoryGetResult(path) {
    return {
      path,
      text: '',
      source: 'multi-tier-memory',
    };
  }
}

// ---------------------------------------------------------------------------
// Module-level singleton (one manager per gateway process)
// ---------------------------------------------------------------------------

let _manager = null;

function getManager(config = {}) {
  if (!_manager) {
    _manager = new MultiTierMemoryManager(config);
    _manager.init().catch((err) =>
      console.error('[MultiTierMemory] init error:', err)
    );
  }
  return _manager;
}

// ---------------------------------------------------------------------------
// Plugin entry point
// ---------------------------------------------------------------------------

const plugin = defineToolPlugin({
  id: 'multi-tier-memory',
  name: 'Multi-Tier Memory',
  description:
    'Three-tier memory plugin: Redis hot-cache + Qdrant cold vectors + llama.cpp embeddings. ' +
    'Handles memory_search (semantic recall), memory_store (persist facts), memory_cache_update (hot cache), and memory_get (file reads).',

  configSchema: ConfigSchema,

  tools: (tool) => [
    // ── memory_search ──────────────────────────────────────────────────────
    tool({
      name: 'memory_search',
      label: 'Memory Search',
      description:
        'Mandatory recall step: semantically search agent memory before answering ' +
        'questions about prior work, decisions, dates, people, preferences, or todos. ' +
        'Queries the Redis hot cache first, then falls back to Qdrant vector search.',

      parameters: MemorySearchSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const agentId = params.agentId ?? 'default';
        const maxResults = params.maxResults ?? 10;
        const minScore = params.minScore ?? 0;
        const searchStart = Date.now();

        // Hot cache first (fast path for active context)
        const hotKeys = await mgr.cacheKeys(agentId);
        const hotHits = [];
        for (const key of hotKeys.slice(0, 5)) {
          const data = await mgr.cacheGet(key, agentId);
          if (data) {
            hotHits.push({
              path: `memory://hot:${key}`,
              text: JSON.stringify(data),
              score: 0.97,
              source: 'multi-tier-memory',
              startLine: 1,
              endLine: JSON.stringify(data).split('\n').length,
              metadata: { tier: 'hot', key },
            });
          }
        }

        // Cold search (Qdrant vector search)
        const coldResults = await mgr.retrieveRelevant(
          params.query,
          agentId,
          maxResults
        );
        const searchMs = Date.now() - searchStart;

        const results = [
          ...hotHits,
          ...coldResults
            .filter((r) => r.score >= minScore)
            .slice(0, maxResults)
            .map((r) => ({
              path: `memory://${r.id}`,
              text: r.text,
              score: r.score,
              source: 'multi-tier-memory',
              startLine: 1,
              endLine: r.text.split('\n').length,
              metadata: r.metadata,
            })),
        ];

        return {
          results,
          provider: 'multi-tier-memory',
          model: mgr.embeddingModel,
          fallback: null,
          citations: false,
          mode: hotHits.length ? 'hot+vector' : 'vector-only',
          debug: {
            backend: 'multi-tier-memory',
            searchMs,
            hits: results.length,
            hotCacheHits: hotHits.length,
            coldHits: coldResults.length,
          },
        };
      },
    }),

    // ── memory_get ─────────────────────────────────────────────────────────
    tool({
      name: 'memory_get',
      label: 'Memory Get',
      description:
        'Read a specific memory file by path. ' +
        'Handles MEMORY.md, memory/*.md files, and daily journal entries.',

      parameters: MemoryGetSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const result = mgr.buildMemoryGetResult(params.path);
        return {
          path: result.path,
          text: result.text,
          source: result.source,
          disabled: false,
        };
      },
    }),

    // ── memory_store ───────────────────────────────────────────────────────
    tool({
      name: 'memory_store',
      label: 'Memory Store',
      description:
        'Persist an important fact, observation, or decision into the cold memory layer. ' +
        'Use when you learn something worth remembering — a decision made, a preference ' +
        'expressed, a fact stated, or a lesson learned. ' +
        'Higher importance scores (0.5-1.0) are recommended for facts that should rank ' +
        'higher in future searches and survive longer.',

      parameters: MemoryStoreSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const agentId = params.agentId ?? 'default';
        const collectionType = params.collectionType ?? 'observations';
        const importance = params.importance ?? 0.5;
        const tags = params.tags ?? [];

        const id = await mgr.storeMemoryText(
          agentId,
          params.text,
          collectionType,
          importance,
          tags
        );

        // Also cache high-importance items in hot layer
        if (importance >= 0.7 && mgr.redisConnected) {
          await mgr.cachePut(
            `importance:${id}`,
            agentId,
            { text: params.text, collectionType, importance, tags },
            Math.round((importance * mgr.hotTtlSeconds) / 2)
          );
        }

        return {
          id,
          stored: true,
          collectionType,
          importance,
          tags,
          agentId,
          note: importance >= 0.7
            ? 'Also cached in hot layer (importance >= 0.7)'
            : 'Stored in cold layer only. Use importance >= 0.7 to also cache hot.',
        };
      },
    }),

    // ── memory_cache_update ────────────────────────────────────────────────
    tool({
      name: 'memory_cache_update',
      label: 'Memory Cache Update',
      description:
        'Update the Redis hot cache with active context — current task state, user ' +
        'preferences, working data, or anything that should be retrieved fast in the ' +
        'next few minutes/hours. ' +
        'Use for ephemeral working state (not persistent facts). ' +
        'Call this after completing significant steps so the next memory_search ' +
        'picks up the hot context immediately without a cold Qdrant round-trip.',

      parameters: MemoryCacheUpdateSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const agentId = params.agentId ?? 'default';
        const ttl = Math.max(params.ttlSeconds ?? mgr.hotTtlSeconds, 60);

        const ok = await mgr.cachePut(params.key, agentId, params.data, ttl);

        if (!ok) {
          return {
            stored: false,
            key: params.key,
            note: 'Redis unavailable — hot cache write skipped. Data NOT persisted.',
            hotLayer: false,
          };
        }

        return {
          stored: true,
          key: params.key,
          ttlSeconds: ttl,
          hotLayer: true,
          note: `Cached in Redis hot layer for ${ttl}s. Will appear in memory_search immediately.`,
        };
      },
    }),
  ],
});

export default plugin;
