/**
 * TaskEventBus — OpenClaw event bus + task queue plugin
 *
 * Architecture:
 *  - Redis Pub/Sub  → inter-agent events, real-time pub/sub channels
 *  - Redis Lists    → task queue (lpush → brpop)
 *  - Redis Keys     → correlation IDs, result storage, pub/sub state
 *
 * Plugin tools:
 *  event_publish    → publish an event to a channel
 *  event_subscribe  → subscribe to a channel (one-shot or persistent)
 *  task_dispatch    → dispatch a task to a named queue, get correlationId
 *  task_subscribe   → worker-side: claim a task from a queue (blocking)
 *  task_results     → poll for a task result by correlationId
 */

import { createRequire } from 'module';
import { defineToolPlugin } from '/app/node_modules/openclaw/dist/plugin-sdk/tool-plugin.js';

// ---------------------------------------------------------------------------
// Redis (CommonJS)
// ---------------------------------------------------------------------------
const _require = createRequire(import.meta.url);
const { createClient: _createRedisClient } = _require(
  '/home/node/.openclaw/plugins/TaskEventBus/node_modules/redis/dist/index.js'
);

// ---------------------------------------------------------------------------
// Config schema
// ---------------------------------------------------------------------------

const ConfigSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    redisUrl: { type: 'string', default: 'redis://redis:6379' },
    // How long (ms) a task result lives before expiring
    resultTtlMs: { type: 'integer', default: 300000 },
    // Default channel prefix for the event bus
    channelPrefix: { type: 'string', default: 'teb' },
    // Default queue prefix
    queuePrefix: { type: 'string', default: 'tasks' },
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
        'Use ":" as separator. Example channels: events:task.*, events:agent.*, events:system:*',
    },
    payload: {
      type: 'object',
      description: 'JSON-serializable event payload.',
    },
    broadcast: {
      type: 'boolean',
      default: false,
      description:
        'If true, message is also published to a matching wildcard channel. ' +
        'e.g. channel "events:task.created" with broadcast=true also publishes to "events:task.*"',
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
      description: 'Channel to subscribe to. Supports pattern matching with * wildcard.',
    },
    agentId: {
      type: 'string',
      description: 'Agent that owns this subscription. Used as the subscriber identity.',
    },
    timeoutSeconds: {
      type: 'integer',
      minimum: 1,
      default: 30,
      description: 'Max time to wait for a message before returning empty.',
    },
  },
  required: ['channel', 'agentId'],
  additionalProperties: false,
};

const TaskDispatchSchema = {
  type: 'object',
  properties: {
    queue: {
      type: 'string',
      description:
        'Queue name, e.g. "code", "qa", "research". ' +
        'Results stored under correlationId with TTL so agents can poll.',
    },
    payload: {
      type: 'object',
      description: 'Task payload — anything the worker needs to process this task.',
    },
    priority: {
      type: 'integer',
      default: 0,
      description:
        'Higher priority tasks (1-10) are inserted near the front of the queue. ' +
        'Default 0 = normal FIFO.',
    },
    ttlSeconds: {
      type: 'integer',
      minimum: 60,
      default: 300,
      description: 'How long the result should be kept after completion (default 5 min).',
    },
    correlationId: {
      type: 'string',
      description:
        'Optional idempotency key. If provided and a task with this ID was already dispatched, ' +
        'returns the existing correlationId without queueing a duplicate.',
    },
  },
  required: ['queue', 'payload'],
  additionalProperties: false,
};

const TaskSubscribeSchema = {
  type: 'object',
  properties: {
    queues: {
      type: 'array',
      items: { type: 'string' },
      description: 'List of queue names to subscribe to (blocking). First queue with a task wins.',
    },
    timeoutSeconds: {
      type: 'integer',
      minimum: 1,
      default: 60,
      description: 'How long to block waiting for a task (0 = infinite).',
    },
    workerId: {
      type: 'string',
      description: 'Unique ID for this worker. Used to track which worker claimed which task.',
    },
  },
  required: ['queues', 'workerId'],
  additionalProperties: false,
};

const TaskResultsSchema = {
  type: 'object',
  properties: {
    correlationId: {
      type: 'string',
      description: 'Correlation ID returned by task_dispatch.',
    },
    clear: {
      type: 'boolean',
      default: false,
      description: 'If true, consume and delete the result after reading.',
    },
  },
  required: ['correlationId'],
  additionalProperties: false,
};

// ---------------------------------------------------------------------------
// EventBusManager
// ---------------------------------------------------------------------------

class EventBusManager {
  constructor(config = {}) {
    this.redisUrl = config.redisUrl ?? 'redis://redis:6379';
    this.channelPrefix = config.channelPrefix ?? 'teb';
    this.queuePrefix = config.queuePrefix ?? 'tasks';
    this.resultTtlMs = config.resultTtlMs ?? 300000;

    // Main client for all operations
    this.client = _createRedisClient({ url: this.redisUrl });
    // Separate client for pub (Redis requires separate connection for subscribe)
    this.subscriber = _createRedisClient({ url: this.redisUrl });

    this.connected = false;
    this._pendingSubscriptions = new Map(); // channel → { resolve, reject, timeout }
  }

  async init() {
    try {
      await this.client.connect();
      await this.subscriber.connect();
      this.connected = true;
      console.log('[TaskEventBus] Redis connected');

      // Wire up message handler for subscriptions
      this.subscriber.on('pmessage', (pattern, channel, message) => {
        this._dispatchMessage(channel, message);
      });

      console.log('[TaskEventBus] EventBusManager ready');
    } catch (err) {
      console.warn('[TaskEventBus] Redis connection failed:', err?.message ?? err);
      this.connected = false;
    }
  }

  // -------------------------------------------------------------------------
  // Internal subscription routing
  // -------------------------------------------------------------------------

  _dispatchMessage(channel, message) {
    const pending = this._pendingSubscriptions.get(channel);
    if (!pending) return;

    let data;
    try {
      data = JSON.parse(message);
    } catch {
      data = { raw: message };
    }

    // Resolve the oldest pending subscribe for this channel
    const entry = pending.shift();
    if (entry) {
      clearTimeout(entry.timeout);
      entry.resolve({ channel, data });
    }

    // If more pending subs exist, re-arm the timeout
    if (pending.length > 0) {
      entry = pending[0];
      entry.timeout = setTimeout(() => {
        const idx = pending.indexOf(entry);
        if (idx >= 0) pending.splice(idx, 1);
        entry.resolve({ channel, data: null, timedOut: true });
      }, entry.timeoutMs);
    }
  }

  // -------------------------------------------------------------------------
  // Event publish
  // -------------------------------------------------------------------------

  async eventPublish(channel, payload, broadcast = false) {
    if (!this.connected) return { published: false, reason: 'Redis not connected' };

    const fullChannel = `${this.channelPrefix}:${channel}`;
    const message = JSON.stringify(payload);

    await this.client.publish(fullChannel, message);

    // Optional wildcard broadcast: publish to matching wildcard patterns
    const broadcastChannels = [];
    if (broadcast && channel.includes('.')) {
      // e.g. "task.created" → also publish to "task.*"
      const [ns, event] = channel.split('.');
      const wildcard = `${this.channelPrefix}:${ns}.*`;
      await this.client.publish(wildcard, message);
      broadcastChannels.push(wildcard);
    }

    return { published: true, channel: fullChannel, broadcastChannels };
  }

  // -------------------------------------------------------------------------
  // Event subscribe (one-shot with timeout)
  // -------------------------------------------------------------------------

  async eventSubscribe(channel, agentId, timeoutSeconds = 30) {
    if (!this.connected) return { received: false, reason: 'Redis not connected' };

    const fullChannel = `${this.channelPrefix}:${channel}`;

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        const pending = this._pendingSubscriptions.get(fullChannel);
        if (pending) {
          const idx = pending.indexOf(entry);
          if (idx >= 0) pending.splice(idx, 1);
        }
        resolve({ received: false, timedOut: true, channel: fullChannel });
      }, timeoutSeconds * 1000);

      const entry = { resolve, timeout, timeoutMs: timeoutSeconds * 1000 };

      if (!this._pendingSubscriptions.has(fullChannel)) {
        this._pendingSubscriptions.set(fullChannel, []);
        // Subscribe to the channel/pattern
        if (channel.includes('*')) {
          this.subscriber.pSubscribe(`${this.channelPrefix}:${channel}`);
        } else {
          this.subscriber.subscribe(fullChannel);
        }
      }

      this._pendingSubscriptions.get(fullChannel).push(entry);
    });
  }

  // -------------------------------------------------------------------------
  // Task dispatch
  // -------------------------------------------------------------------------

  async taskDispatch(queue, payload, priority = 0, ttlSeconds = 300, correlationId = null) {
    if (!this.connected) return { dispatched: false, reason: 'Redis not connected' };

    // Idempotency: if correlationId provided and exists, skip dispatch
    if (correlationId) {
      const existing = await this.client.get(`teb:result:${correlationId}`);
      if (existing) {
        const result = JSON.parse(existing);
        return { dispatched: false, idempotent: true, correlationId, existingResult: result };
      }
    }

    const id = correlationId ?? `teb-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const fullQueue = `${this.queuePrefix}:${queue}`;

    const task = {
      correlationId: id,
      queue,
      payload,
      priority,
      createdAt: new Date().toISOString(),
    };

    if (priority > 0) {
      // Priority: store in a sorted set, scored by priority (higher = earlier)
      await this.client.zAdd(fullQueue, {
        score: 1000 - priority, // invert so highest priority = lowest score = comes first
        value: JSON.stringify(task),
      });
    } else {
      // Normal FIFO: push to list head
      await this.client.lPush(fullQueue, JSON.stringify(task));
    }

    // Expose result key for polling
    await this.client.setEx(`teb:result:${id}`, ttlSeconds, JSON.stringify({ status: 'pending' }));

    return { dispatched: true, correlationId: id, queue: fullQueue };
  }

  // -------------------------------------------------------------------------
  // Task subscribe (worker-side blocking pop)
  // -------------------------------------------------------------------------

  async taskSubscribe(queues, workerId, timeoutSeconds = 60) {
    if (!this.connected) return { received: false, reason: 'Redis not connected' };

    const fullQueues = queues.map((q) => `${this.queuePrefix}:${q}`);

    // First try priority queues (sorted set) for each queue
    for (const q of fullQueues) {
      const task = await this.client.zRange(q, 0, 0); // peek highest priority
      if (task.length > 0) {
        await this.client.zRem(q, task[0]);
        const parsed = JSON.parse(task[0]);
        // Mark as processing
        await this.client.setEx(
          `teb:result:${parsed.correlationId}`,
          300,
          JSON.stringify({ status: 'processing', workerId, startedAt: new Date().toISOString() })
        );
        return { received: true, task: parsed, queue: q.replace(`${this.queuePrefix}:`, '') };
      }
    }

    // Then try normal queues with brpop
    if (timeoutSeconds > 0) {
      // Note: brpopMulti doesn't exist in older redis, use brpop on each
      for (const q of fullQueues) {
        try {
          const result = await this.client.brPop(q, timeoutSeconds);
          if (result) {
            const parsed = JSON.parse(result.element);
            await this.client.setEx(
              `teb:result:${parsed.correlationId}`,
              300,
              JSON.stringify({ status: 'processing', workerId, startedAt: new Date().toISOString() })
            );
            return {
              received: true,
              task: parsed,
              queue: q.replace(`${this.queuePrefix}:`, ''),
            };
          }
        } catch {
          // brPopTimeout = no item available, try next queue
        }
      }
    }

    return { received: false, timedOut: true };
  }

  // -------------------------------------------------------------------------
  // Task result update (worker calls this on completion)
  // -------------------------------------------------------------------------

  async taskComplete(correlationId, result, ttlSeconds = 300) {
    if (!this.connected) return false;
    await this.client.setEx(
      `teb:result:${correlationId}`,
      ttlSeconds,
      JSON.stringify({ status: 'completed', result, completedAt: new Date().toISOString() })
    );
    return true;
  }

  async taskFail(correlationId, error, ttlSeconds = 300) {
    if (!this.connected) return false;
    await this.client.setEx(
      `teb:result:${correlationId}`,
      ttlSeconds,
      JSON.stringify({ status: 'failed', error: String(error), failedAt: new Date().toISOString() })
    );
    return true;
  }

  // -------------------------------------------------------------------------
  // Task results poll (agent-side)
  // -------------------------------------------------------------------------

  async taskResults(correlationId, clear = false) {
    if (!this.connected) return { found: false, reason: 'Redis not connected' };
    const key = `teb:result:${correlationId}`;
    const raw = await this.client.get(key);
    if (!raw) return { found: false, reason: 'No result found for this correlationId' };

    const result = JSON.parse(raw);

    if (clear) {
      await this.client.del(key);
    }

    return { found: true, ...result };
  }
}

// ---------------------------------------------------------------------------
// Module-level singleton
// ---------------------------------------------------------------------------

let _manager = null;

function getManager(config = {}) {
  if (!_manager) {
    _manager = new EventBusManager(config);
    _manager.init().catch((err) =>
      console.error('[TaskEventBus] init error:', err)
    );
  }
  return _manager;
}

// ---------------------------------------------------------------------------
// Plugin entry point
// ---------------------------------------------------------------------------

const plugin = defineToolPlugin({
  id: 'task-event-bus',
  name: 'Task & Event Bus',
  description:
    'Redis-backed task queue and event bus. ' +
    'event_publish/subscribe for pub/sub between agents. ' +
    'task_dispatch/subscribe/results for async task processing. ' +
    'Workers are standalone processes; agents dispatch tasks and poll for results.',

  configSchema: ConfigSchema,

  tools: (tool) => [
    // ── event_publish ──────────────────────────────────────────────────────
    tool({
      name: 'event_publish',
      label: 'Event Publish',
      description:
        'Publish an event to a channel. Any agent or process subscribed to the channel ' +
        'receives it immediately. Use for inter-agent notifications, system events, ' +
        'or triggering workflows. ' +
        'Example: publish("events:task.created", { task: "build", agent: "code-agent" })',

      parameters: EventPublishSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const { channel, payload, broadcast } = params;
        const result = await mgr.eventPublish(channel, payload, broadcast ?? false);
        return result;
      },
    }),

    // ── event_subscribe ───────────────────────────────────────────────────
    tool({
      name: 'event_subscribe',
      label: 'Event Subscribe',
      description:
        'Subscribe to an event channel and wait for the next message (one-shot). ' +
        'Timeout returns empty if no message arrives. ' +
        'Supports wildcard channels: "events:task.*" matches "events:task.created", ' +
        '"events:task.completed", etc. ' +
        'For persistent listening, call repeatedly with short timeouts.',

      parameters: EventSubscribeSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const { channel, agentId, timeoutSeconds } = params;
        const result = await mgr.eventSubscribe(
          channel,
          agentId ?? 'unknown',
          timeoutSeconds ?? 30
        );
        return result;
      },
    }),

    // ── task_dispatch ─────────────────────────────────────────────────────
    tool({
      name: 'task_dispatch',
      label: 'Task Dispatch',
      description:
        'Dispatch a task to a named queue. Returns a correlationId for polling ' +
        'the result later with task_results. ' +
        'Workers (standalone processes) consume from these queues via task_subscribe. ' +
        'Queues: "code", "qa", "research", "ui", "general". ' +
        'Use priority=1-10 to jump ahead in queue for urgent tasks.',

      parameters: TaskDispatchSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const { queue, payload, priority, ttlSeconds, correlationId } = params;
        const result = await mgr.taskDispatch(
          queue,
          payload,
          priority ?? 0,
          ttlSeconds ?? 300,
          correlationId ?? null
        );
        return result;
      },
    }),

    // ── task_subscribe (worker-facing) ───────────────────────────────────
    tool({
      name: 'task_subscribe',
      label: 'Task Subscribe',
      description:
        'Worker-side: block-wait for a task from one or more queues. ' +
        'Returns the task payload and marks it as "processing". ' +
        'After completing the work, the worker MUST call task_results with ' +
        'status=completed (via a separate internal mechanism or direct Redis write). ' +
        'This tool is primarily for worker processes, not agents.',

      parameters: TaskSubscribeSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const { queues, workerId, timeoutSeconds } = params;
        const result = await mgr.taskSubscribe(
          queues,
          workerId ?? 'unknown',
          timeoutSeconds ?? 60
        );
        return result;
      },
    }),

    // ── task_results ──────────────────────────────────────────────────────
    tool({
      name: 'task_results',
      label: 'Task Results',
      description:
        'Poll for a task result by correlationId. ' +
        'Returns { status: "pending" | "completed" | "failed", result?, error? }. ' +
        'Agents should poll this every few seconds after task_dispatch. ' +
        'Set clear=true to consume and delete the result after reading (idempotent).',

      parameters: TaskResultsSchema,

      async execute(params, config) {
        const mgr = getManager(config ?? {});
        const { correlationId, clear } = params;
        const result = await mgr.taskResults(correlationId, clear ?? false);
        return result;
      },
    }),
  ],
});

export default plugin;
