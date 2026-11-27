/**
 * Streams one `agy-multi acp` turn into Archon `MessageChunk`s.
 *
 * Bridges ACP `session/update` notifications + `session/prompt` response:
 * - Coalesces `agent_message_chunk` and `agent_thought_chunk` deltas at block,
 *   tool, and turn boundaries.
 * - Ensures every `tool_call` is emitted once and closed once by a terminal
 *   `tool_call_update` (filtering out ACP non-terminal `pending`/`in_progress` updates).
 * - Checks `session/list` when `session/resume` fails so missing sessions report
 *   `sessionNotFound: true` from structured session IDs rather than error prose.
 */
import { z } from 'zod';
import {
  clampEffort,
  providerStopReasonSchema,
  sessionPreview,
  truncateToolOutput,
  type ProviderStopReason,
} from '@archon/provider-contract';
import { createLogger } from '@archon/paths';
import { resolveSkillDirectories } from '@archon/paths/skills';
import type {
  MessageChunk,
  ProviderWarning,
  ResultChunk,
  SendQueryOptions,
  TokenUsage,
} from '../types';
import { loadMcpConfig } from '../mcp/config';
import { ClassifiedProviderError } from '../shared/failure';
import { augmentPromptForJsonSchema, tryParseStructuredOutput } from '../shared/structured-output';
import { resumedOutcome, withResumedOutcome } from '../shared/resumed';
import { AcpJsonRpcError, AgyMultiAcpClient, type AcpNotification } from './acp-client';
import type { AgyMultiProviderDefaults } from './config';

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.agy-multi.stream');
  return cachedLog;
}

const AGY_MULTI_ACP_EFFORTS = ['low', 'medium', 'high', 'xhigh'] as const;
type AgyMultiAcpEffort = (typeof AGY_MULTI_ACP_EFFORTS)[number];

class AsyncQueue<T> implements AsyncIterable<T> {
  private readonly buffer: T[] = [];
  private readonly waiters: ((result: IteratorResult<T>) => void)[] = [];
  private closed = false;

  push(item: T): void {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: item, done: false });
    else this.buffer.push(item);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift();
      if (waiter) waiter({ value: undefined, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.iterate();
  }

  private async *iterate(): AsyncGenerator<T> {
    while (true) {
      const next = this.buffer.shift();
      if (next !== undefined) {
        yield next;
        continue;
      }
      if (this.closed) return;
      const result = await new Promise<IteratorResult<T>>(resolve => {
        this.waiters.push(resolve);
      });
      if (result.done) return;
      yield result.value;
    }
  }
}

const textContentSchema = z.object({ type: z.literal('text'), text: z.string() });

const sessionConfigOptionSchema = z.object({
  id: z.string(),
  category: z.string().optional(),
  currentValue: z.string().optional(),
  options: z.array(z.object({ value: z.string(), name: z.string().optional() })).optional(),
});

const sessionOpenResponseSchema = z.object({
  sessionId: z.string().min(1),
  configOptions: z.array(sessionConfigOptionSchema).optional(),
});

const sessionListResponseSchema = z.object({
  sessions: z.array(z.object({ sessionId: z.string() })).optional(),
});

const promptResultSchema = z.object({
  stopReason: z.string().optional(),
  usage: z
    .object({
      inputTokens: z.number().int().nonnegative().optional(),
      outputTokens: z.number().int().nonnegative().optional(),
      cachedReadTokens: z.number().int().nonnegative().optional(),
      cachedWriteTokens: z.number().int().nonnegative().optional(),
    })
    .optional(),
});

function resolveSystemAppend(requestOptions?: SendQueryOptions): string | undefined {
  const explicitPrompt = requestOptions?.systemPrompt ?? requestOptions?.nodeConfig?.systemPrompt;
  const raw = explicitPrompt ?? requestOptions?.appendSystemPrompt;
  const text = Array.isArray(raw) ? raw.join('\n') : raw;
  return typeof text === 'string' && text.length > 0 ? text : undefined;
}

function extractToolOutput(update: Record<string, unknown>): {
  text?: string;
  exitCode?: number;
} {
  let exitCode: number | undefined;
  let text: string | undefined;

  const rawOutput = update.rawOutput;
  if (typeof rawOutput === 'string') {
    text = rawOutput;
  } else if (rawOutput !== null && typeof rawOutput === 'object') {
    const record = rawOutput as Record<string, unknown>;
    if (typeof record.exit_code === 'number' && Number.isInteger(record.exit_code)) {
      exitCode = record.exit_code;
    } else if (typeof record.exitCode === 'number' && Number.isInteger(record.exitCode)) {
      exitCode = record.exitCode;
    }
    if (typeof record.output === 'string') {
      text = record.output;
    } else if (typeof record.stdout === 'string') {
      text = record.stdout;
    } else if (typeof record.error === 'string') {
      text = record.error;
    } else {
      text = JSON.stringify(rawOutput);
    }
  }

  if (!text && Array.isArray(update.content)) {
    const parts: string[] = [];
    for (const block of update.content) {
      if (block === null || typeof block !== 'object') continue;
      const b = block as { type?: unknown; text?: unknown; content?: unknown };
      if (b.type === 'text' && typeof b.text === 'string') {
        parts.push(b.text);
      } else if (b.type === 'content' && b.content !== null && typeof b.content === 'object') {
        const inner = b.content as { type?: unknown; text?: unknown };
        if (inner.type === 'text' && typeof inner.text === 'string') {
          parts.push(inner.text);
        }
      }
    }
    if (parts.length > 0) text = parts.join('\n');
  }

  return {
    ...(text !== undefined ? { text } : {}),
    ...(exitCode !== undefined ? { exitCode } : {}),
  };
}

function mapStopReason(raw: string | undefined): ProviderStopReason {
  const parsed = providerStopReasonSchema.safeParse(raw);
  return parsed.success ? parsed.data : 'end_turn';
}

function mapUsage(raw: z.infer<typeof promptResultSchema>['usage']): TokenUsage | undefined {
  if (!raw || typeof raw.inputTokens !== 'number' || typeof raw.outputTokens !== 'number') {
    return undefined;
  }
  return {
    input: raw.inputTokens,
    output: raw.outputTokens,
    ...(typeof raw.cachedReadTokens === 'number' ? { cacheRead: raw.cachedReadTokens } : {}),
    ...(typeof raw.cachedWriteTokens === 'number' ? { cacheWrite: raw.cachedWriteTokens } : {}),
  };
}

function resolveThoughtLevel(
  configOptions: z.infer<typeof sessionConfigOptionSchema>[] | undefined,
  desired: AgyMultiAcpEffort
): string {
  const option = configOptions?.find(c => c.id === 'thought_level');
  const allowed = option?.options?.map(o => o.value);
  if (!allowed || allowed.length === 0 || allowed.includes(desired)) return desired;
  if (desired === 'xhigh' && allowed.includes('high')) return 'high';
  return allowed.includes('medium') ? 'medium' : allowed[0];
}

type AcpNameValuePair = { name: string; value: string };

type AcpMcpServer =
  | { name: string; command: string; args: string[]; env: AcpNameValuePair[] }
  | { type: 'http' | 'sse'; name: string; url: string; headers: AcpNameValuePair[] };

function toNameValuePairs(raw: unknown, serverName: string, field: string): AcpNameValuePair[] {
  if (raw === undefined) return [];
  if (Array.isArray(raw)) {
    return raw.map(entry => {
      if (
        entry !== null &&
        typeof entry === 'object' &&
        typeof (entry as { name?: unknown }).name === 'string' &&
        typeof (entry as { value?: unknown }).value === 'string'
      ) {
        return {
          name: (entry as { name: string }).name,
          value: (entry as { value: string }).value,
        };
      }
      throw new ClassifiedProviderError(
        'misconfigured',
        `MCP server "${serverName}".${field} array entries must have string "name" and "value"`
      );
    });
  }
  if (typeof raw === 'object' && raw !== null) {
    return Object.entries(raw as Record<string, unknown>).map(([name, value]) => {
      if (typeof value !== 'string') {
        throw new ClassifiedProviderError(
          'misconfigured',
          `MCP server "${serverName}".${field}.${name} must be a string`
        );
      }
      return { name, value };
    });
  }
  throw new ClassifiedProviderError(
    'misconfigured',
    `MCP server "${serverName}".${field} must be an object or array of {name, value}`
  );
}

function toAcpMcpServers(servers: Record<string, unknown>): AcpMcpServer[] {
  const result: AcpMcpServer[] = [];
  for (const [name, raw] of Object.entries(servers)) {
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
      throw new ClassifiedProviderError(
        'misconfigured',
        `MCP server "${name}" must be a JSON object`
      );
    }
    const config = raw as Record<string, unknown>;
    if (typeof config.command === 'string' && config.command.length > 0) {
      const args = Array.isArray(config.args)
        ? config.args.filter((arg): arg is string => typeof arg === 'string')
        : [];
      result.push({
        name,
        command: config.command,
        args,
        env: toNameValuePairs(config.env, name, 'env'),
      });
      continue;
    }
    const url =
      typeof config.url === 'string' && config.url.length > 0
        ? config.url
        : typeof config.serverUrl === 'string' && config.serverUrl.length > 0
          ? config.serverUrl
          : undefined;
    if (url) {
      const type = config.type === 'sse' ? 'sse' : 'http';
      result.push({
        type,
        name,
        url,
        headers: toNameValuePairs(config.headers, name, 'headers'),
      });
      continue;
    }
    throw new ClassifiedProviderError(
      'misconfigured',
      `MCP server "${name}" must specify either "command" (stdio) or "url" (http/sse)`
    );
  }
  return result;
}

async function resolveNodeMcpServers(
  cwd: string,
  requestOptions?: SendQueryOptions
): Promise<{ mcpServers: AcpMcpServer[]; warnings: ProviderWarning[] }> {
  const mcpPath = requestOptions?.nodeConfig?.mcp;
  if (typeof mcpPath !== 'string' || mcpPath.length === 0) {
    return { mcpServers: [], warnings: [] };
  }
  const envSource = requestOptions?.env ? { ...process.env, ...requestOptions.env } : process.env;
  const { servers, serverNames, missingVars } = await loadMcpConfig(mcpPath, cwd, envSource);
  const warnings: ProviderWarning[] = [];
  if (missingVars.length > 0) {
    const uniqueVars = [...new Set(missingVars)];
    warnings.push({
      code: 'agy_multi.mcp_env_vars_missing',
      message: `agy-multi MCP config references undefined env vars: ${uniqueVars.join(', ')}. Servers using them may fail at runtime.`,
    });
  }
  const mcpServers = toAcpMcpServers(servers);
  getLog().info({ serverNames, missingVars }, 'agy_multi.mcp_loaded');
  return { mcpServers, warnings };
}

function resolveNodeSkills(
  cwd: string,
  requestOptions?: SendQueryOptions
): { skillPaths: string[]; warnings: ProviderWarning[] } {
  const declared = requestOptions?.nodeConfig?.skills;
  if (!declared || declared.length === 0) {
    return { skillPaths: [], warnings: [] };
  }
  const { paths, missing } = resolveSkillDirectories(cwd, declared);
  const warnings: ProviderWarning[] = [];
  if (missing.length > 0) {
    warnings.push({
      code: 'agy_multi.skills_missing',
      message: `agy-multi ignored missing skills: ${missing.join(', ')}. Expected a directory with SKILL.md under .agents/skills/ or .claude/skills/ (project or home).`,
    });
  }
  getLog().info({ resolved: paths.length, missing }, 'agy_multi.skills_resolved');
  return { skillPaths: paths, warnings };
}

export function buildAgyMultiAcpArgs(
  config: AgyMultiProviderDefaults,
  requestOptions?: SendQueryOptions
): string[] {
  const isWorkflowNode =
    requestOptions?.nodeConfig?.nodeId !== undefined ||
    requestOptions?.purpose === 'title-generation';
  const disableCustomizations = isWorkflowNode || config.clean === true;
  const additionalDirs = config.additionalDirectories ?? [];

  return [
    'acp',
    ...(disableCustomizations ? ['--clean'] : []),
    ...(config.compactionThreshold !== undefined
      ? ['--compaction-threshold', String(config.compactionThreshold)]
      : []),
    ...(config.switchDelay !== undefined ? ['--switch-delay', String(config.switchDelay)] : []),
    ...(config.handoffContext !== undefined
      ? ['--handoff-context', String(config.handoffContext)]
      : []),
    '--',
    '--dangerously-skip-permissions',
    ...(disableCustomizations ? ['--include-default-customizations=false'] : []),
    ...additionalDirs.flatMap(dir => ['--add-dir', dir]),
  ];
}

type StreamItem =
  | { kind: 'notification'; notification: AcpNotification }
  | { kind: 'done'; result: unknown }
  | { kind: 'error'; error: unknown };

export async function* streamAgyMultiTurn(
  client: AgyMultiAcpClient,
  prompt: string,
  cwd: string,
  resumeSessionId: string | undefined,
  config: AgyMultiProviderDefaults,
  requestOptions?: SendQueryOptions
): AsyncGenerator<MessageChunk> {
  const abortSignal = requestOptions?.abortSignal;
  const { mcpServers, warnings: mcpWarnings } = await resolveNodeMcpServers(cwd, requestOptions);
  const { skillPaths, warnings: skillWarnings } = resolveNodeSkills(cwd, requestOptions);
  for (const warning of [...mcpWarnings, ...skillWarnings]) {
    yield { type: 'warning', ...warning };
  }

  await client.request(
    'initialize',
    {
      protocolVersion: 1,
      clientCapabilities: {
        fs: { readTextFile: false, writeTextFile: false },
        terminal: false,
      },
    },
    abortSignal
  );

  const systemAppend = resolveSystemAppend(requestOptions);
  let sessionId: string;
  let configOptions: z.infer<typeof sessionConfigOptionSchema>[] | undefined;
  let resumed: boolean | undefined;

  if (resumeSessionId && requestOptions?.forkSession) {
    const raw = await client.request(
      'session/fork',
      { sessionId: resumeSessionId, cwd },
      abortSignal
    );
    const parsed = sessionOpenResponseSchema.parse(raw);
    sessionId = parsed.sessionId;
    configOptions = parsed.configOptions;
    resumed = true;
  } else if (resumeSessionId) {
    try {
      const raw = await client.request(
        'session/resume',
        {
          sessionId: resumeSessionId,
          cwd,
          ...(mcpServers.length > 0 ? { mcpServers } : {}),
        },
        abortSignal
      );
      const parsed = sessionOpenResponseSchema.parse(raw);
      sessionId = parsed.sessionId;
      configOptions = parsed.configOptions;
      resumed = true;
    } catch (error) {
      if (error instanceof AcpJsonRpcError && !abortSignal?.aborted) {
        const missing = await isSessionMissing(client, resumeSessionId, cwd, abortSignal);
        if (missing) {
          yield {
            type: 'result',
            isError: true,
            sessionNotFound: true,
            errorSubtype: 'session_not_found',
            errors: [error.message],
            resumed: false,
            failure: { class: 'unknown', evidence: error.message },
          };
          return;
        }
      }
      throw error;
    }
  } else {
    const antigravityMeta: Record<string, unknown> = {
      ...(systemAppend ? { systemPrompt: { append: systemAppend } } : {}),
      ...(skillPaths.length > 0 ? { skills: skillPaths } : {}),
    };
    const raw = await client.request(
      'session/new',
      {
        cwd,
        mcpServers,
        ...(Object.keys(antigravityMeta).length > 0
          ? { _meta: { antigravity: antigravityMeta } }
          : {}),
      },
      abortSignal
    );
    const parsed = sessionOpenResponseSchema.parse(raw);
    sessionId = parsed.sessionId;
    configOptions = parsed.configOptions;
    resumed = undefined;
  }

  getLog().debug(
    { sessionId: sessionPreview(sessionId), resumed: resumed ?? false },
    'agy_multi.session_ready'
  );
  yield { type: 'session', sessionId };

  const requestedModel = requestOptions?.model?.trim() || config.model?.trim() || undefined;
  if (requestedModel) {
    await client.request('session/set_model', { sessionId, modelId: requestedModel }, abortSignal);
  }

  const rawEffort = requestOptions?.nodeConfig?.effort ?? config.effort;
  const clampedEffort = clampEffort(rawEffort, AGY_MULTI_ACP_EFFORTS);
  if (rawEffort !== undefined && clampedEffort !== rawEffort) {
    getLog().debug({ declared: rawEffort, applied: clampedEffort }, 'agy_multi.effort_clamped');
  }
  if (clampedEffort) {
    const thoughtLevel = resolveThoughtLevel(configOptions, clampedEffort);
    try {
      await client.request(
        'session/set_config_option',
        { sessionId, configId: 'thought_level', value: thoughtLevel },
        abortSignal
      );
    } catch (error) {
      if (!(error instanceof AcpJsonRpcError && error.code === -32601)) throw error;
    }
  }

  const outputSchema =
    requestOptions?.outputFormat?.type === 'json_schema'
      ? requestOptions.outputFormat.schema
      : undefined;
  const effectivePrompt = outputSchema ? augmentPromptForJsonSchema(prompt, outputSchema) : prompt;

  yield* withResumedOutcome(
    streamPromptTurn(client, sessionId, effectivePrompt, outputSchema, abortSignal),
    resumedOutcome(resumeSessionId, resumed === true)
  );
}

async function isSessionMissing(
  client: AgyMultiAcpClient,
  sessionId: string,
  cwd: string,
  abortSignal?: AbortSignal
): Promise<boolean> {
  try {
    const raw = await client.request('session/list', { cwd }, abortSignal);
    const parsed = sessionListResponseSchema.safeParse(raw);
    if (!parsed.success || !parsed.data.sessions) return false;
    return !parsed.data.sessions.some(session => session.sessionId === sessionId);
  } catch {
    return false;
  }
}

async function* streamPromptTurn(
  client: AgyMultiAcpClient,
  sessionId: string,
  prompt: string,
  outputSchema: Record<string, unknown> | undefined,
  abortSignal?: AbortSignal
): AsyncGenerator<MessageChunk> {
  const queue = new AsyncQueue<StreamItem>();
  const unsubscribe = client.onNotification(notification => {
    queue.push({ kind: 'notification', notification });
  });

  const onAbort = (): void => {
    client.notify('session/cancel', { sessionId });
  };
  abortSignal?.addEventListener('abort', onAbort, { once: true });

  void client
    .request('session/prompt', { sessionId, prompt: [{ type: 'text', text: prompt }] }, abortSignal)
    .then(
      result => {
        queue.push({ kind: 'done', result });
        queue.close();
      },
      error => {
        queue.push({ kind: 'error', error });
        queue.close();
      }
    );

  let pendingText = '';
  let pendingThought = '';
  let fullReply = '';
  const startedTools = new Set<string>();
  const closedTools = new Set<string>();

  function* flushThought(): Generator<MessageChunk> {
    if (pendingThought.length > 0) {
      const text = pendingThought;
      pendingThought = '';
      yield { type: 'agent_thought_chunk', text };
    }
  }

  function* flushText(): Generator<MessageChunk> {
    if (pendingText.length > 0) {
      const text = pendingText;
      pendingText = '';
      yield { type: 'agent_message_chunk', text };
    }
  }

  try {
    for await (const item of queue) {
      if (abortSignal?.aborted) throw new Error('Query aborted');

      if (item.kind === 'error') {
        throw item.error;
      }

      if (item.kind === 'done') {
        yield* flushThought();
        yield* flushText();
        const promptRes = promptResultSchema.parse(item.result);
        const structuredOutput = outputSchema ? tryParseStructuredOutput(fullReply) : undefined;
        const tokens = mapUsage(promptRes.usage);
        const resultChunk: ResultChunk = {
          type: 'result',
          sessionId,
          text: fullReply,
          stopReason: mapStopReason(promptRes.stopReason),
          ...(structuredOutput !== undefined ? { structuredOutput } : {}),
          ...(tokens ? { tokens } : {}),
        };
        yield resultChunk;
        return;
      }

      const { method, params } = item.notification;
      if (method !== 'session/update' || params === null || typeof params !== 'object') continue;
      const p = params as { sessionId?: unknown; update?: unknown };
      if (p.sessionId !== undefined && p.sessionId !== sessionId) continue;
      if (p.update === null || typeof p.update !== 'object') continue;
      const update = p.update as Record<string, unknown>;
      const kind = update.sessionUpdate;

      if (kind === 'agent_message_chunk') {
        const content = textContentSchema.safeParse(update.content);
        if (content.success && content.data.text.length > 0) {
          yield* flushThought();
          pendingText += content.data.text;
          fullReply += content.data.text;
        }
      } else if (kind === 'agent_thought_chunk') {
        const content = textContentSchema.safeParse(update.content);
        if (content.success && content.data.text.length > 0) {
          yield* flushText();
          pendingThought += content.data.text;
        }
      } else if (kind === 'tool_call') {
        const toolCallId = typeof update.toolCallId === 'string' ? update.toolCallId : '';
        if (!toolCallId) continue;
        yield* flushThought();
        yield* flushText();
        if (!startedTools.has(toolCallId)) {
          startedTools.add(toolCallId);
          const name =
            typeof update.name === 'string' && update.name.length > 0
              ? update.name
              : typeof update.title === 'string' && update.title.length > 0
                ? update.title
                : 'tool';
          yield {
            type: 'tool_call',
            toolCallId,
            name,
            ...(typeof update.title === 'string' ? { title: update.title } : {}),
            ...(update.rawInput !== undefined ? { rawInput: update.rawInput as never } : {}),
          };
        }
      } else if (kind === 'tool_call_update') {
        const toolCallId = typeof update.toolCallId === 'string' ? update.toolCallId : '';
        const status = update.status;
        if (!toolCallId) continue;
        if (status !== 'completed' && status !== 'failed' && status !== 'cancelled') continue;
        yield* flushThought();
        yield* flushText();
        if (!startedTools.has(toolCallId)) {
          startedTools.add(toolCallId);
          const name =
            typeof update.name === 'string' && update.name.length > 0
              ? update.name
              : typeof update.title === 'string' && update.title.length > 0
                ? update.title
                : 'tool';
          yield {
            type: 'tool_call',
            toolCallId,
            name,
            ...(typeof update.title === 'string' ? { title: update.title } : {}),
            ...(update.rawInput !== undefined ? { rawInput: update.rawInput as never } : {}),
          };
        }
        if (!closedTools.has(toolCallId)) {
          closedTools.add(toolCallId);
          const { text, exitCode } = extractToolOutput(update);
          const truncated = text !== undefined ? truncateToolOutput(text) : undefined;
          yield {
            type: 'tool_call_update',
            toolCallId,
            status,
            ...(truncated ? { output: truncated.output } : {}),
            ...(truncated?.outputTruncated ? { outputTruncated: true } : {}),
            ...(exitCode !== undefined ? { exitCode } : {}),
          };
        }
      }
    }
  } finally {
    abortSignal?.removeEventListener('abort', onAbort);
    unsubscribe();
    queue.close();
  }
}
