/**
 * TaskEventBus — OpenClaw event bus + task queue plugin
 *
 * Architecture:
 *  - Redis Pub/Sub  → inter-agent events, real-time pub/sub channels
 *  - Redis Lists    → task queue (lpush → brpop)
 *  - Redis Keys     → correlation IDs, result storage
 *
 * Plugin tools:
 *  event_publish    → publish an event to a channel
 *  event_subscribe  → subscribe to a channel (one-shot)
 *  task_dispatch    → dispatch a task to a named queue, get correlationId
 *  task_results     → poll for a task result by correlationId
 */

import { createRequire } from 'module';

// ---------------------------------------------------------------------------
// Redis (CommonJS, loaded via createRequire for compatibility)
// ---------------------------------------------------------------------------
const _require = createRequire(import.meta.url);
const { createClient: _createRedisClient } = _require('redis');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ConfigSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    redisUrl: {
      type: 'string',
      default: 'redis://redis:6379',
      description: 'Redis connection URL',
    },
    resultTtlMs: {
      type: 'integer',
      default: 300000,
      description: 'How long (ms) a task result lives before expiring',
    },
    channelPrefix: {
      type: 'string',
      default: 'teb',
      description: 'Prefix for pub/sub channel names',
    },
    queuePrefix: {
      type: 'string',
      default: 'tasks',
      description: 'Prefix for task queue list keys',
    },
  },
};

// ---------------------------------------------------------------------------
// Tool schemas
// ---------------------------------------------------------------------------

const EventPublishSchema = {
  type: 'object',
  properties: {
    channel: {
      type: 'string',
      description:
        'Channel name, e.g. "events:task.created", "events:agent.ui". ' +
        'Use ":" as separator.',
    },
    payload: {
      type: 'object',
      description: 'Arbitrary JSON payload to publish',
    },
  },
  required: ['channel', 'payload'],
  additionalProperties: false,
};

const EventSubscribeSchema = {
  type: 'object',
  properties: {
    channel: {
      type: 'string',
      description: 'Channel name to subscribe to',
    },
    timeoutMs: {
      type: 'integer',
      default: 5000,
      description: 'How long to wait for a message before returning (ms)',
    },
  },
  required: ['channel'],
  additionalProperties: false,
};

const TaskDispatchSchema = {
  type: 'object',
  properties: {
    queue: {
      type: 'string',
      default: 'general',
      description: 'Queue name to dispatch to (e.g. general, code, qa, research, ui)',
    },
    payload: {
      type: 'object',
      description: 'Task payload. Must include "type" field: "code-run", "web-fetch", "agent-run"',
    },
    priority: {
      type: 'integer',
      minimum: 1,
      maximum: 10,
      default: 0,
      description: 'Priority 1-10 (1=highest). Default 0 = standard FIFO queue',
    },
    correlationId: {
      type: 'string',
      description: 'Optional idempotency ID. Auto-generated if not provided',
    },
    ttlSeconds: {
      type: 'integer',
      minimum: 1,
      description: 'Result TTL override (default from config)',
    },
  },
  required: ['payload'],
  additionalProperties: false,
};

const TaskResultsSchema = {
  type: 'object',
  properties: {
    correlationId: {
      type: 'string',
      description: 'Correlation ID returned by task_dispatch',
    },
    wait: {
      type: 'integer',
      default: 0,
      description: 'Max ms to wait for a result if not yet available',
    },
  },
  required: ['correlationId'],
  additionalProperties: false,
};

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

class TaskEventBusPlugin {
  constructor(config) {
    this.redisUrl = config.redisUrl ?? 'redis://redis:6379';
    this.resultTtlMs = config.resultTtlMs ?? 300000;
    this.channelPrefix = config.channelPrefix ?? 'teb';
    this.queuePrefix = config.queuePrefix ?? 'tasks';
    /** @type {import('redis').RedisClientType | null} */
    this._pubClient = null;
    /** @type {import('redis').RedisClientType | null} */
    this._subClient = null;
  }

  async _getPubClient() {
    if (!this._pubClient) {
      this._pubClient = _createRedisClient({ url: this.redisUrl });
      this._pubClient.on('error', (e) => console.error('[TEB Redis pub]', e.message));
      await this._pubClient.connect();
    }
    return this._pubClient;
  }

  async _getSubClient() {
    if (!this._subClient) {
      // Separate connection required for subscribe (subscribe mode is exclusive)
      this._subClient = _createRedisClient({ url: this.redisUrl });
      this._subClient.on('error', (e) => console.error('[TEB Redis sub]', e.message));
      await this._subClient.connect();
    }
    return this._subClient;
  }

  async event_publish({ channel, payload } = {}) {
    try {
      const fullChannel = channel.startsWith(this.channelPrefix) ? channel : `${this.channelPrefix}:${channel}`;
      const client = await this._getPubClient();
      const message = JSON.stringify({ payload, publishedAt: new Date().toISOString() });
      const subscriberCount = await client.publish(fullChannel, message);
      return { channel: fullChannel, subscriberCount, payload };
    } catch (err) {
      return { error: err.message, channel };
    }
  }

  async event_subscribe({ channel, timeoutMs = 5000 } = {}) {
    try {
      const fullChannel = channel.startsWith(this.channelPrefix) ? channel : `${this.channelPrefix}:${channel}`;
      const client = await this._getSubClient();

      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          client.unsubscribe(fullChannel);
          resolve({ channel: fullChannel, message: null, timedOut: true });
        }, timeoutMs);

        client.subscribe(fullChannel, (message) => {
          clearTimeout(timer);
          client.unsubscribe(fullChannel);
          try {
            const parsed = JSON.parse(message);
            resolve({ channel: fullChannel, message: parsed, timedOut: false });
          } catch {
            resolve({ channel: fullChannel, message: { raw: message }, timedOut: false });
          }
        });

        client.on('error', (err) => {
          clearTimeout(timer);
          reject(err);
        });
      });
    } catch (err) {
      return { error: err.message, channel };
    }
  }

  async task_dispatch({ queue = 'general', payload, priority = 0, correlationId, ttlSeconds } = {}) {
    try {
      const id = correlationId ?? `teb-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const fullQueue = `${this.queuePrefix}:${queue}`;

      const task = {
        correlationId: id,
        queue,
        payload,
        priority,
        createdAt: new Date().toISOString(),
      };

      const client = await this._getPubClient();
      const ttl = ttlSeconds ?? Math.floor(this.resultTtlMs / 1000);

      // Idempotency: skip if already dispatched (result key already exists)
      const resultKey = `teb:result:${id}`;
      const existing = await client.get(resultKey);
      if (existing) {
        const parsed = JSON.parse(existing);
        return { correlationId: id, skipped: true, currentStatus: parsed.status };
      }

      // Push to queue (priority 1-10 goes to sorted set; 0 = standard FIFO)
      if (priority >= 1 && priority <= 10) {
        // Priority queue: score = priority (lower = higher priority)
        await client.zAdd(fullQueue, { score: priority, value: JSON.stringify(task) });
      } else {
        // Standard FIFO
        await client.lPush(fullQueue, JSON.stringify(task));
      }

      // Initialize result as pending
      await client.setEx(resultKey, ttl, JSON.stringify({ status: 'pending', correlationId: id }));

      return { correlationId: id, queue, priority, dispatchedAt: task.createdAt };
    } catch (err) {
      return { error: err.message };
    }
  }

  async task_results({ correlationId, wait = 0 } = {}) {
    try {
      const client = await this._getPubClient();
      const resultKey = `teb:result:${correlationId}`;

      // Poll up to `wait` ms for a non-pending result
      const deadline = Date.now() + wait;
      while (true) {
        const raw = await client.get(resultKey);
        if (raw) {
          const result = JSON.parse(raw);
          if (result.status !== 'pending') return result;
        }
        if (Date.now() >= deadline) break;
        await new Promise((r) => setTimeout(r, 200));
      }

      // Return current state (might still be pending)
      const raw = await client.get(resultKey);
      if (raw) return JSON.parse(raw);
      return { status: 'not_found', correlationId };
    } catch (err) {
      return { error: err.message, correlationId };
    }
  }

  async initialize() {
    await Promise.all([this._getPubClient(), this._getSubClient()]);
    console.log('[TaskEventBus] Initialized');
  }

  async destroy() {
    if (this._pubClient) { await this._pubClient.quit(); this._pubClient = null; }
    if (this._subClient) { await this._subClient.quit(); this._subClient = null; }
  }
}

// ---------------------------------------------------------------------------
// Plugin registration
// ---------------------------------------------------------------------------

import { defineToolPlugin } from 'openclaw';

export default defineToolPlugin({
  name: 'task-event-bus',
  version: '1.0.0',
  config: ConfigSchema,

  tools: [
    {
      name: 'event_publish',
      description: 'Publish an event to a Redis pub/sub channel',
      schema: EventPublishSchema,
      handler: (args, ctx) => ctx.plugin.event_publish(args),
    },
    {
      name: 'event_subscribe',
      description: 'Subscribe to a Redis pub/sub channel (one-shot, with timeout)',
      schema: EventSubscribeSchema,
      handler: (args, ctx) => ctx.plugin.event_subscribe(args),
    },
    {
      name: 'task_dispatch',
      description: 'Dispatch an async task to a Redis queue, get a correlationId',
      schema: TaskDispatchSchema,
      handler: (args, ctx) => ctx.plugin.task_dispatch(args),
    },
    {
      name: 'task_results',
      description: 'Poll for a task result by correlationId',
      schema: TaskResultsSchema,
      handler: (args, ctx) => ctx.plugin.task_results(args),
    },
  ],

  factory: (config) => new TaskEventBusPlugin(config),
});
