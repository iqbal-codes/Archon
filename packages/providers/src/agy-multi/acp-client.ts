/**
 * JSON-RPC 2.0 stdio client for `agy-multi acp`.
 *
 * Spawns one `agy-multi acp` process per turn, speaks newline-delimited JSON-RPC,
 * buffers redacted stderr as failure evidence, and handles server-initiated
 * requests (`session/request_permission`, unsupported `fs/*` / `terminal/*`).
 */
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface, type Interface as ReadlineInterface } from 'node:readline';
import { createLogger } from '@archon/paths';
import {
  collectCredentialValues,
  redactCredentialValues,
} from '@archon/paths/credential-redaction';
import { terminateTree } from '@archon/paths/plugin-process';

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> | undefined {
  if (!cachedLog) cachedLog = createLogger('provider.agy-multi.acp');
  return cachedLog;
}

export type AcpProcessGroupOwner = 'connection' | 'host';

export type AcpSpawner = (
  command: string,
  args: string[],
  options: { env: Record<string, string>; stdio: ['pipe', 'pipe', 'pipe']; detached: boolean }
) => ChildProcessWithoutNullStreams;

export class AcpJsonRpcError extends Error {
  constructor(
    readonly method: string,
    readonly code: number,
    message: string,
    readonly data?: unknown
  ) {
    super(`${method} failed (JSON-RPC ${String(code)}): ${message}`);
    this.name = 'AcpJsonRpcError';
  }
}

export type AcpConnectionEnd =
  | { kind: 'exited'; code: number | null; signal: NodeJS.Signals | null }
  | { kind: 'spawn_failed'; error: NodeJS.ErrnoException };

const STDERR_BUFFER_CHARS = 8000;
const STDERR_EVIDENCE_LINES = 10;
const STDERR_EVIDENCE_CHARS = 1000;
const SHUTDOWN_GRACE_MS = 3000;

function describeEnd(end: AcpConnectionEnd): string {
  if (end.kind === 'spawn_failed') return `could not start: ${end.error.message}`;
  return `exited (${end.signal ? `signal ${end.signal}` : `code ${String(end.code)}`})`;
}

function stderrEvidence(buffer: string, credentialValues: readonly string[]): string {
  const lines = redactCredentialValues(buffer, credentialValues).trim().split('\n');
  return lines.slice(-STDERR_EVIDENCE_LINES).join('\n').slice(-STDERR_EVIDENCE_CHARS);
}

export class AcpConnectionClosedError extends Error {
  readonly #stderr: string;

  constructor(
    readonly end: AcpConnectionEnd,
    readonly beforeFirstResponse: boolean,
    stderr: string
  ) {
    super(`agy-multi acp ${describeEnd(end)} before the turn completed`);
    this.name = 'AcpConnectionClosedError';
    this.#stderr = stderr;
  }

  get evidence(): string {
    return this.#stderr ? `${this.message}\nstderr:\n${this.#stderr}` : this.message;
  }
}

export interface AcpNotification {
  method: string;
  params: unknown;
}

interface PendingRequest {
  method: string;
  resolve: (result: unknown) => void;
  reject: (error: Error) => void;
}

function defaultPermissionOutcome(params: unknown): { outcome: Record<string, unknown> } {
  const options =
    params !== null &&
    typeof params === 'object' &&
    Array.isArray((params as { options?: unknown }).options)
      ? ((params as { options: unknown[] }).options as { optionId?: unknown; kind?: unknown }[])
      : [];
  const preferred =
    options.find(opt => opt.kind === 'allow_always') ??
    options.find(opt => opt.kind === 'allow_once') ??
    options[0];
  if (typeof preferred?.optionId === 'string' && preferred.optionId.length > 0) {
    return { outcome: { outcome: 'selected', optionId: preferred.optionId } };
  }
  return { outcome: { outcome: 'cancelled' } };
}

export class AgyMultiAcpClient {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly rl: ReadlineInterface;
  private readonly pending = new Map<number, PendingRequest>();
  private readonly notificationListeners = new Set<(notification: AcpNotification) => void>();
  private readonly credentialValues: readonly string[];
  private readonly pid: number | undefined;
  private readonly closedPromise: Promise<AcpConnectionEnd>;
  private nextId = 1;
  private stderrBuffer = '';
  private responded = false;
  private end: AcpConnectionEnd | undefined;
  private closing = false;

  constructor(options: {
    binaryPath: string;
    args: string[];
    env: Record<string, string>;
    protectedEnvKeys?: readonly string[];
    spawner?: AcpSpawner;
    processGroupOwner?: AcpProcessGroupOwner;
  }) {
    const spawner: AcpSpawner =
      options.spawner ?? ((cmd, args, opts) => spawn(cmd, args, { ...opts, windowsHide: true }));
    this.credentialValues = collectCredentialValues(options.env, options.protectedEnvKeys);
    this.child = spawner(options.binaryPath, options.args, {
      env: options.env,
      stdio: ['pipe', 'pipe', 'pipe'],
      detached: options.processGroupOwner !== 'host' && process.platform !== 'win32',
    });
    this.pid = this.child.pid;
    this.child.stdin.on('error', () => {
      /* Pending requests observe close/exit; broken pipe writes fail on close. */
    });
    this.child.stderr.on('data', (chunk: Buffer) => {
      const text = chunk.toString('utf8');
      this.stderrBuffer = (this.stderrBuffer + text).slice(-STDERR_BUFFER_CHARS);
    });
    this.rl = createInterface({ input: this.child.stdout, crlfDelay: Infinity });
    this.rl.on('line', line => this.handleLine(line));

    this.closedPromise = new Promise<AcpConnectionEnd>(resolve => {
      let settled = false;
      const finish = (end: AcpConnectionEnd): void => {
        if (settled) return;
        settled = true;
        this.end = end;
        this.rl.close();
        this.rejectAllPending(end);
        resolve(end);
      };
      this.child.once('error', error => {
        finish({ kind: 'spawn_failed', error: error as NodeJS.ErrnoException });
      });
      this.child.once('close', (code, signal) => {
        finish({ kind: 'exited', code, signal });
      });
    });
  }

  onNotification(listener: (notification: AcpNotification) => void): () => void {
    this.notificationListeners.add(listener);
    return () => {
      this.notificationListeners.delete(listener);
    };
  }

  request<T = unknown>(method: string, params: unknown, signal?: AbortSignal): Promise<T> {
    if (this.end) {
      return Promise.reject(
        new AcpConnectionClosedError(
          this.end,
          !this.responded,
          stderrEvidence(this.stderrBuffer, this.credentialValues)
        )
      );
    }
    if (signal?.aborted) {
      return Promise.reject(new Error('Query aborted'));
    }

    const id = this.nextId++;
    return new Promise<T>((resolve, reject) => {
      const onAbort = (): void => {
        this.pending.delete(id);
        reject(new Error('Query aborted'));
      };
      signal?.addEventListener('abort', onAbort, { once: true });

      this.pending.set(id, {
        method,
        resolve: value => {
          signal?.removeEventListener('abort', onAbort);
          resolve(value as T);
        },
        reject: err => {
          signal?.removeEventListener('abort', onAbort);
          reject(err);
        },
      });

      this.writeLine({ jsonrpc: '2.0', id, method, params });
    });
  }

  notify(method: string, params: unknown): void {
    if (this.end) return;
    this.writeLine({ jsonrpc: '2.0', method, params });
  }

  async close(): Promise<void> {
    if (this.closing) return;
    this.closing = true;
    if (!this.end) {
      this.child.stdin.end();
      const settled = await Promise.race([
        this.closedPromise.then(() => true),
        new Promise<false>(resolve => setTimeout(() => resolve(false), SHUTDOWN_GRACE_MS)),
      ]);
      if (!settled && this.pid !== undefined) {
        try {
          await terminateTree(this.pid);
        } catch (error) {
          getLog()?.debug({ err: error }, 'agy_multi.terminate_tree_failed');
        }
      }
    }
    await this.closedPromise;
  }

  private writeLine(payload: Record<string, unknown>): void {
    try {
      this.child.stdin.write(`${JSON.stringify(payload)}\n`);
    } catch {
      /* Closed stdin will be reported via closedPromise. */
    }
  }

  private handleLine(rawLine: string): void {
    const trimmed = rawLine.trim();
    if (!trimmed) return;
    let parsed: unknown;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      getLog()?.debug({ bytes: trimmed.length }, 'agy_multi.non_json_stdout_line');
      return;
    }
    if (parsed === null || typeof parsed !== 'object') return;
    const msg = parsed as {
      id?: number | string;
      method?: unknown;
      params?: unknown;
      result?: unknown;
      error?: { code?: unknown; message?: unknown; data?: unknown };
    };

    if (typeof msg.method === 'string' && msg.id !== undefined) {
      this.handleServerRequest(msg.id, msg.method, msg.params);
      return;
    }

    if (typeof msg.method === 'string') {
      const notification: AcpNotification = { method: msg.method, params: msg.params };
      for (const listener of this.notificationListeners) listener(notification);
      return;
    }

    if (typeof msg.id === 'number') {
      const pending = this.pending.get(msg.id);
      if (!pending) return;
      this.pending.delete(msg.id);
      this.responded = true;
      if (msg.error) {
        const code = typeof msg.error.code === 'number' ? msg.error.code : -32603;
        const message =
          typeof msg.error.message === 'string' ? msg.error.message : 'Unknown JSON-RPC error';
        pending.reject(new AcpJsonRpcError(pending.method, code, message, msg.error.data));
      } else {
        pending.resolve(msg.result);
      }
    }
  }

  private handleServerRequest(id: number | string, method: string, params: unknown): void {
    if (method === 'session/request_permission') {
      this.writeLine({ jsonrpc: '2.0', id, result: defaultPermissionOutcome(params) });
      return;
    }
    this.writeLine({
      jsonrpc: '2.0',
      id,
      error: { code: -32601, message: `Unsupported client method: ${method}` },
    });
  }

  private rejectAllPending(end: AcpConnectionEnd): void {
    if (this.pending.size === 0) return;
    const error = new AcpConnectionClosedError(
      end,
      !this.responded,
      stderrEvidence(this.stderrBuffer, this.credentialValues)
    );
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
  }
}
