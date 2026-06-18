#!/usr/bin/env node
/**
 * TaskEventBus Worker
 *
 * Standalone process that consumes tasks from Redis queues and processes them.
 * Designed to run as a separate Docker container (survives OpenClaw restarts).
 *
 * Usage:
 *   node worker.js [--queues general,code,qa,research,ui]
 *
 * Environment variables:
 *   TEB_REDIS_URL        Redis URL (default: redis://redis:6379)
 *   TEB_QUEUE_PREFIX     Queue key prefix (default: tasks)
 *   TEB_RESULT_PREFIX    Result key prefix (default: teb:result)
 *   TEB_WORKER_ID        Unique worker ID (auto-generated if unset)
 *   TEB_QUEUES           Comma-separated queue names (default: general,code,qa,research,ui)
 *   TEB_POLL_INTERVAL    Poll interval ms when queue is empty (default: 3000)
 *   TEB_RESULT_TTL       Result TTL seconds (default: 300)
 *   TEB_LOG_LEVEL        Log level: debug|info|warn|error (default: info)
 */

import { createRequire } from 'module';

const _require = createRequire(import.meta.url);
const { createClient: _createRedisClient } = _require('redis');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const CONFIG = {
  redisUrl: process.env.TEB_REDIS_URL ?? 'redis://redis:6379',
  queuePrefix: process.env.TEB_QUEUE_PREFIX ?? 'tasks',
  resultPrefix: process.env.TEB_RESULT_PREFIX ?? 'teb:result',
  workerId:
    process.env.TEB_WORKER_ID ??
    `worker-${process.env.HOSTNAME ?? 'node'}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
  queues: (process.env.TEB_QUEUES ?? 'general,code,qa,research,ui')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),
  pollIntervalMs: parseInt(process.env.TEB_POLL_INTERVAL ?? '3000', 10),
  resultTtlSeconds: parseInt(process.env.TEB_RESULT_TTL ?? '300', 10),
  logLevel: process.env.TEB_LOG_LEVEL ?? 'info',
};

const LOG = {
  debug: (...a) => CONFIG.logLevel === 'debug' && console.debug(`[${ts()}] [DEBUG]`, ...a),
  info: (...a) => ['debug', 'info'].includes(CONFIG.logLevel) && console.info(`[${ts()}] [INFO]`, ...a),
  warn: (...a) => console.warn(`[${ts()}] [WARN]`, ...a),
  error: (...a) => console.error(`[${ts()}] [ERROR]`, ...a),
};

function ts() {
  return new Date().toISOString().slice(11, 23);
}

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--queues' && args[i + 1]) {
    CONFIG.queues = args[i + 1].split(',').map((s) => s.trim());
    i++;
  }
  if (args[i] === '--worker-id' && args[i + 1]) {
    CONFIG.workerId = args[i + 1];
    i++;
  }
}

LOG.info(`Worker ID: ${CONFIG.workerId}`);
LOG.info(`Queues: ${CONFIG.queues.join(', ')}`);
LOG.info(`Redis: ${CONFIG.redisUrl}`);

// ---------------------------------------------------------------------------
// Task handlers
// ---------------------------------------------------------------------------

const handlers = {
  /**
   * code-run — execute a shell command
   * Payload: { command: string, timeoutSeconds?: number }
   */
  async 'code-run'(payload) {
    if (process.env.TEB_ALLOW_CODE_RUN !== 'true') {
      return { success: false, error: 'code-run is disabled. Set TEB_ALLOW_CODE_RUN=true to enable.' };
    }
    const { command, timeoutSeconds = 60 } = payload;
    return new Promise((resolve) => {
      const start = Date.now();
      // Node.js child_process
      const { spawn } = _require('child_process');
      const child = spawn('/bin/sh', ['-c', command], { stdio: 'pipe' });
      let stdout = '';
      let stderr = '';

      child.stdout.on('data', (d) => (stdout += d.toString()));
      child.stderr.on('data', (d) => (stderr += d.toString()));

      const timer = setTimeout(() => {
        child.kill('SIGKILL');
        resolve({ success: false, error: 'TIMEOUT', stdout, stderr, timedOut: true });
      }, timeoutSeconds * 1000);

      child.on('close', (code) => {
        clearTimeout(timer);
        resolve({
          success: code === 0,
          code,
          stdout,
          stderr,
          durationMs: Date.now() - start,
        });
      });
      child.on('error', (err) => {
        clearTimeout(timer);
        resolve({ success: false, error: err.message });
      });
    });
  },

  /**
   * web-fetch — fetch a URL and return content
   * Payload: { url: string, method?: string, headers?: object, body?: string }
   */
  async 'web-fetch'(payload) {
    const { url, method = 'GET', headers = {}, body, timeoutSeconds = 30 } = payload;
    const start = Date.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutSeconds * 1000);
    try {
      const res = await fetch(url, { method, headers, body, redirect: 'follow', signal: controller.signal });
      const text = await res.text();
      clearTimeout(timer);
      return {
        success: true,
        status: res.status,
        statusText: res.statusText,
        headers: Object.fromEntries(res.headers.entries()),
        body: text.slice(0, 10000), // cap at 10k chars
        durationMs: Date.now() - start,
      };
    } catch (err) {
      clearTimeout(timer);
      const isTimeout = err.name === 'AbortError';
      return { success: false, error: isTimeout ? 'TIMEOUT' : err.message, timedOut: isTimeout, durationMs: Date.now() - start };
    }
  },

  /**
   * agent-run — placeholder for spawning an OpenClaw sub-agent
   * Payload: { prompt: string, agentId?: string }
   */
  async 'agent-run'(payload) {
    return {
      success: false,
      error: 'agent-run handler not implemented — register with registerHandler()',
    };
  },
};

/** Register a custom task handler */
export function registerHandler(type, fn) {
  handlers[type] = fn;
  LOG.info(`Registered handler: ${type}`);
}

// ---------------------------------------------------------------------------
// Redis
// ---------------------------------------------------------------------------

let redis;

async function initRedis() {
  redis = _createRedisClient({ url: CONFIG.redisUrl });
  redis.on('error', (e) => LOG.error('[Redis]', e.message));
  await redis.connect();
  LOG.info('Redis connected');
}

async function claimTask() {
  if (CONFIG.queues.length === 0) return null;

  // 1. Try to pop from priority queue (sorted set) first
  for (const queue of CONFIG.queues) {
    const priorityQueue = `${CONFIG.queuePrefix}:${queue}:priority`;
    try {
      const result = await redis.zPopMin(priorityQueue);
      if (result) {
        return JSON.parse(result.value);
      }
    } catch (err) {
      LOG.error(`Error claiming priority task from ${priorityQueue}:`, err.message);
    }
  }

  // 2. Try brpop (blocking right-pop) with a short timeout on normal queues simultaneously
  const normalQueues = CONFIG.queues.map((q) => `${CONFIG.queuePrefix}:${q}`);
  try {
    const result = await redis.brPop(normalQueues, 2);
    if (result) {
      return JSON.parse(result.element);
    }
  } catch (err) {
    // Only log if it's not a timeout error
    if (!err.message?.includes('null')) {
      LOG.error(`Error claiming normal task:`, err.message);
    }
  }

  return null;
}

async function writeResult(correlationId, data) {
  const key = `${CONFIG.resultPrefix}:${correlationId}`;
  await redis.setEx(key, CONFIG.resultTtlSeconds, JSON.stringify(data));
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

async function processTask(task) {
  const { correlationId, payload, queue } = task;
  LOG.info(`[${correlationId}] Processing ${payload?.type ?? 'unknown'} from ${queue}`);

  // Mark as processing
  await writeResult(correlationId, {
    status: 'processing',
    workerId: CONFIG.workerId,
    startedAt: new Date().toISOString(),
  });

  const handler = handlers[payload?.type];
  if (!handler) {
    const result = { status: 'failed', error: `Unknown custom handler: ${payload?.type}`, workerId: CONFIG.workerId };
    await writeResult(correlationId, { ...result, completedAt: new Date().toISOString() });
    LOG.warn(`[${correlationId}] No handler for type: ${payload?.type}`);
    return;
  }

  try {
    const output = await handler(payload);
    await writeResult(correlationId, {
      status: 'completed',
      result: output,
      workerId: CONFIG.workerId,
      completedAt: new Date().toISOString(),
    });
    LOG.info(`[${correlationId}] Completed`);
  } catch (err) {
    await writeResult(correlationId, {
      status: 'failed',
      error: err.message,
      workerId: CONFIG.workerId,
      completedAt: new Date().toISOString(),
    });
    LOG.error(`[${correlationId}] Failed:`, err.message);
  }
}

let lastQueueRefresh = 0;
const REFRESH_INTERVAL_MS = 15000;

async function refreshQueues() {
  const now = Date.now();
  if (now - lastQueueRefresh < REFRESH_INTERVAL_MS) {
    return;
  }
  lastQueueRefresh = now;

  try {
    const registryKey = `${CONFIG.queuePrefix}:registry`;
    const registered = await redis.sMembers(registryKey);

    // Baseline queues from env config
    const envQueues = (process.env.TEB_QUEUES ?? 'general,code,qa,research,ui')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    // Merge baseline and dynamically registered queues
    const allQueues = Array.from(new Set([...envQueues, ...registered]));

    const prevStr = CONFIG.queues.sort().join(',');
    const newStr = allQueues.sort().join(',');
    if (prevStr !== newStr) {
      CONFIG.queues = allQueues;
      LOG.info(`Dynamically updated queues list: ${CONFIG.queues.join(', ')}`);
    }
  } catch (err) {
    LOG.error(`Failed to refresh queues from Redis:`, err.message);
  }
}

async function run() {
  await initRedis();
  await refreshQueues();
  LOG.info(`Listening on queues: ${CONFIG.queues.join(', ')}`);

  while (true) {
    try {
      await refreshQueues();
      const task = await claimTask();
      if (task) {
        await processTask(task);
      } else {
        // No task found and brPop timed out — wait pollIntervalMs if queues are empty,
        // otherwise claimTask's block of 2s acts as the natural throttling.
        if (CONFIG.queues.length === 0) {
          await new Promise((r) => setTimeout(r, CONFIG.pollIntervalMs));
        }
      }
    } catch (err) {
      LOG.error(`Error in worker loop:`, err.message);
      await new Promise((r) => setTimeout(r, CONFIG.pollIntervalMs));
    }
  }
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

run().catch((err) => {
  LOG.error('Worker crashed:', err);
  process.exit(1);
});
