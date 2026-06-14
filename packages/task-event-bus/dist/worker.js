#!/usr/bin/env node
/**
 * TaskEventBus Worker
 *
 * Standalone process that consumes tasks from Redis queues and processes them.
 * Run as: node worker.js [--queues code,qa,research] [--worker-id worker-1] [--poll-interval 5]
 *
 * For each task:
 *  1. Claims it from the queue (brpop / zpop)
 *  2. Calls the appropriate handler based on task.type
 *  3. Writes result back to Redis under teb:result:{correlationId}
 *  4. Optionally publishes an event on completion
 *
 * Supports task types:
 *  - "agent-run"  → spawn a sub-agent session with the given prompt
 *  - "code-run"    → run a shell command, capture output
 *  - "web-fetch"   → fetch a URL, return content
 *  - "custom"      → invoke a registered handler function
 */

import { createRequire } from 'module';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Config (from env or defaults)
// ---------------------------------------------------------------------------

const CONFIG = {
  redisUrl: process.env.TEB_REDIS_URL ?? 'redis://redis:6379',
  queuePrefix: process.env.TEB_QUEUE_PREFIX ?? 'tasks',
  resultPrefix: 'teb:result',
  // Auto-generate unique worker ID if not set:
  //   - HOSTNAME env var is set automatically by Docker
  //   - random suffix prevents restarts within the same second from colliding
  workerId: process.env.TEB_WORKER_ID
    ?? `worker-${process.env.HOSTNAME ?? 'node'}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
  queues: (process.env.TEB_QUEUES ?? 'general,code,qa,research,ui').split(',').map((s) => s.trim()),
  pollIntervalMs: parseInt(process.env.TEB_POLL_INTERVAL ?? '3000', 10),
  resultTtlSeconds: parseInt(process.env.TEB_RESULT_TTL ?? '300', 10),
  logLevel: process.env.TEB_LOG_LEVEL ?? 'info',
};

// ---------------------------------------------------------------------------
// Redis
// ---------------------------------------------------------------------------

const _require = createRequire(import.meta.url);
const { createClient: _createRedisClient } = _require(
  resolve(__dirname, '../node_modules/redis/dist/index.js')
);

const redis = _createRedisClient({ url: CONFIG.redisUrl });

function log(level, ...args) {
  if (CONFIG.logLevel === 'silent') return;
  const prefix = `[${new Date().toISOString()}] [${level.toUpperCase()}] [${CONFIG.workerId}]`;
  console[level === 'error' ? 'error' : 'log'](prefix, ...args);
}

// ---------------------------------------------------------------------------
// Task handlers
// ---------------------------------------------------------------------------

const handlers = {
  /**
   * agent-run: Spawn a sub-agent session with the given task
   * Requires openclaw CLI access from this machine
   */
  async 'agent-run'(task, ctx) {
    const { agentId, prompt, model, timeoutSeconds } = task.payload;
    const correlationId = task.correlationId;

    log('info', `agent-run: spawning ${agentId} for task ${correlationId}`);

    try {
      // Build the sessions_spawn command
      const { spawn } = await import('child_process');
      const args = [
        'sessions',
        'spawn',
        '--agent-id', agentId,
        '--message', prompt,
        '--session-key', `teb-task-${correlationId}`,
      ];
      if (model) args.push('--model', model);
      if (timeoutSeconds) args.push('--timeout-seconds', String(timeoutSeconds));

      // For now, run as a detached process and poll for result
      // In a full implementation, this would use OpenClaw's internal API
      const { exec } = await import('child_process');

      return new Promise((resolve) => {
        const cmd = `openclaw sessions spawn --agent-id "${agentId}" --message "${prompt.replace(/"/g, '\\"')}" --session-key "teb-task-${correlationId}" --timeout-seconds ${timeoutSeconds ?? 60}`;
        exec(cmd, { cwd: process.env.OPENCLAW_HOME ?? '/home/node/.openclaw' }, (err, stdout, stderr) => {
          if (err) {
            resolve({ success: false, error: err.message, stderr });
          } else {
            resolve({ success: true, stdout: stdout.slice(0, 2000) });
          }
        });
      });
    } catch (err) {
      return { success: false, error: err.message };
    }
  },

  /**
   * code-run: Execute a shell command
   */
  async 'code-run'(task, ctx) {
    const { command, cwd, timeoutSeconds = 60, env = {} } = task.payload;
    const correlationId = task.correlationId;

    log('info', `code-run: executing: ${command.slice(0, 100)} for task ${correlationId}`);

    return new Promise((resolve) => {
      const { exec } = import('child_process').then(({ exec }) => {
        const fullEnv = { ...process.env, ...env };
        exec(
          command,
          { cwd: cwd ?? process.env.OPENCLAW_HOME ?? '/home/node/.openclaw', timeout: (timeoutSeconds ?? 60) * 1000, env: fullEnv },
          (err, stdout, stderr) => {
            if (err) {
              resolve({ success: false, error: err.message, stderr: stderr?.slice(-1000) });
            } else {
              resolve({ success: true, stdout: stdout.slice(-5000), stderr: stderr?.slice(-1000) });
            }
          }
        );
      });
    });
  },

  /**
   * web-fetch: Fetch a URL and return readable content
   */
  async 'web-fetch'(task, ctx) {
    const { url, maxChars = 10000, extractMode = 'markdown' } = task.payload;
    const correlationId = task.correlationId;

    log('info', `web-fetch: fetching ${url} for task ${correlationId}`);

    try {
      const res = await fetch(url, {
        signal: AbortSignal.timeout(30000),
        headers: { 'User-Agent': 'TaskEventBus-Worker/1.0' },
      });

      if (!res.ok) {
        return { success: false, error: `HTTP ${res.status}: ${res.statusText}` };
      }

      const text = await res.text();
      return {
        success: true,
        content: text.slice(0, maxChars),
        status: res.status,
        url,
      };
    } catch (err) {
      return { success: false, error: err.message };
    }
  },

  /**
   * custom: call a registered handler by name
   */
  async 'custom'(task, ctx) {
    const { handler, payload } = task.payload;
    if (ctx.customHandlers && ctx.customHandlers[handler]) {
      return ctx.customHandlers[handler](payload, task);
    }
    return { success: false, error: `Unknown custom handler: ${handler}` };
  },
};

// ---------------------------------------------------------------------------
// Task processor
// ---------------------------------------------------------------------------

async function processTask(task, ctx) {
  const type = task.payload?.type ?? task.payload?.taskType ?? 'unknown';
  const handler = handlers[type] ?? handlers['custom'];

  try {
    const result = await handler(task, ctx);
    return { success: true, result };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

// ---------------------------------------------------------------------------
// Result writer
// ---------------------------------------------------------------------------

async function writeResult(correlationId, result, ttlSeconds) {
  try {
    await redis.setEx(
      `${CONFIG.resultPrefix}:${correlationId}`,
      ttlSeconds ?? CONFIG.resultTtlSeconds,
      JSON.stringify({
        status: 'completed',
        result,
        completedAt: new Date().toISOString(),
        workerId: CONFIG.workerId,
      })
    );
  } catch (err) {
    log('error', `Failed to write result for ${correlationId}:`, err.message);
  }
}

async function writeError(correlationId, error, ttlSeconds) {
  try {
    await redis.setEx(
      `${CONFIG.resultPrefix}:${correlationId}`,
      ttlSeconds ?? CONFIG.resultTtlSeconds,
      JSON.stringify({
        status: 'failed',
        error: String(error),
        failedAt: new Date().toISOString(),
        workerId: CONFIG.workerId,
      })
    );
  } catch (err) {
    log('error', `Failed to write error for ${correlationId}:`, err.message);
  }
}

// ---------------------------------------------------------------------------
// Main worker loop
// ---------------------------------------------------------------------------

const customHandlers = {};

function registerHandler(name, fn) {
  customHandlers[name] = fn;
  log('info', `Registered custom handler: ${name}`);
}

async function pollOnce(ctx) {
  const fullQueues = CONFIG.queues.map((q) => `${CONFIG.queuePrefix}:${q}`);

  // Try priority queues first (sorted sets)
  for (const q of fullQueues) {
    try {
      const tasks = await redis.zRange(q, 0, 0);
      if (tasks.length > 0) {
        const rawTask = tasks[0];
        await redis.zRem(q, rawTask);
        const task = JSON.parse(rawTask);

        log('info', `Claimed task ${task.correlationId} from priority queue ${q}`);

        const result = await processTask(task, ctx);
        if (result.success) {
          await writeResult(task.correlationId, result.result);
        } else {
          await writeError(task.correlationId, result.error);
        }
        return true;
      }
    } catch (err) {
      // Ignore per-queue errors, try next
    }
  }

  // Try normal queues with blocking pop
  for (const q of fullQueues) {
    try {
      const result = await redis.brPop(q, Math.ceil(CONFIG.pollIntervalMs / 1000));
      if (result) {
        const task = JSON.parse(result.element);

        log('info', `Claimed task ${task.correlationId} from queue ${q}`);

        const procResult = await processTask(task, ctx);
        if (procResult.success) {
          await writeResult(task.correlationId, procResult.result);
        } else {
          await writeError(task.correlationId, procResult.error);
        }
        return true;
      }
    } catch (err) {
      if (!err.message?.includes('null')) {
        log('warn', `brpop on ${q}:`, err.message);
      }
    }
  }

  return false;
}

async function main() {
  // Parse CLI args
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--queues' && args[i + 1]) {
      CONFIG.queues = args[i + 1].split(',').map((s) => s.trim());
      i++;
    } else if (args[i] === '--worker-id' && args[i + 1]) {
      CONFIG.workerId = args[i + 1];
      i++;
    } else if (args[i] === '--poll-interval' && args[i + 1]) {
      CONFIG.pollIntervalMs = parseInt(args[i + 1], 10) * 1000;
      i++;
    } else if (args[i] === '--help') {
      console.log(`
TaskEventBus Worker

Usage: node worker.js [options]

Options:
  --queues <list>      Comma-separated queue names (default: general,code,qa,research,ui)
  --worker-id <id>     Unique worker ID (default: auto-generated)
  --poll-interval <s>  Non-blocking poll interval in seconds (default: 3)
  --help               Show this help

Environment variables:
  TEB_REDIS_URL        Redis URL (default: redis://redis:6379)
  TEB_QUEUE_PREFIX     Queue prefix (default: tasks)
  TEB_WORKER_ID        Worker ID (default: auto)
  TEB_QUEUES           Comma-separated queues (default: general,code,qa,research,ui)
  TEB_POLL_INTERVAL    Poll interval ms (default: 3000)
  TEB_RESULT_TTL       Result TTL seconds (default: 300)

Custom handlers:
  Register via: registerHandler('my-handler', (payload, task) => { return { success: true, data: ... }; });
`);
      process.exit(0);
    }
  }

  log('info', `Connecting to Redis: ${CONFIG.redisUrl}`);
  await redis.connect();
  log('info', `Connected. Worker ID: ${CONFIG.workerId}`);
  log('info', `Listening on queues: ${CONFIG.queues.join(', ')}`);

  const ctx = { customHandlers, CONFIG };

  // Main loop
  while (true) {
    try {
      const hadTask = await pollOnce(ctx);
      if (!hadTask) {
        // No task available — sleep briefly before next poll
        await new Promise((r) => setTimeout(r, CONFIG.pollIntervalMs));
      }
    } catch (err) {
      log('error', 'Poll error:', err.message);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

main().catch((err) => {
  log('error', 'Worker fatal error:', err);
  process.exit(1);
});
