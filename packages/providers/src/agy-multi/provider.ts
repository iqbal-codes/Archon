/**
 * agy-multi ACP provider: runs one `agy-multi acp` process per turn over stdio
 * JSON-RPC, exposes diagnostics via `agy-multi profile list` and `--version`,
 * and lists available models via `agy-multi models`.
 */
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type {
  CredentialStatus,
  ProviderDiagnostics,
  ProviderFailureClass,
  ProviderModelList,
} from '@archon/provider-contract';
import { createLogger } from '@archon/paths';
import {
  collectCredentialValues,
  redactCredentialValues,
} from '@archon/paths/credential-redaction';
import type {
  IAgentProvider,
  MessageChunk,
  ProviderCapabilities,
  SendQueryOptions,
} from '../types';
import { ClassifiedProviderError, failureClassOfThrown, failureResult } from '../shared/failure';
import { closeOpenToolCalls } from '../shared/tool-calls';
import { AGY_MULTI_CAPABILITIES } from './capabilities';
import { parseAgyMultiConfig } from './config';
import { resolveAgyMultiBinaryPath, resolveAgyMultiBinaryWithSource } from './binary-resolver';
import {
  AcpConnectionClosedError,
  AcpJsonRpcError,
  AgyMultiAcpClient,
  type AcpProcessGroupOwner,
  type AcpSpawner,
} from './acp-client';
import { buildAgyMultiAcpArgs, streamAgyMultiTurn } from './stream';

const execFileAsync = promisify(execFile);
const SUBPROCESS_TIMEOUT_MS = 10_000;

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('provider.agy-multi');
  return cachedLog;
}

export interface AgyMultiProfileSummary {
  total: number;
  healthy: number;
}

export function parseAgyMultiProfiles(stdout: string): AgyMultiProfileSummary {
  const lines = stdout
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0 && !line.startsWith('#'));
  let healthy = 0;
  for (const line of lines) {
    const columns = line.split('\t');
    if (columns[2]?.trim() === 'ok') healthy++;
  }
  return { total: lines.length, healthy };
}

export function parseAgyMultiModels(stdout: string): { id: string; label?: string }[] {
  const seen = new Map<string, string | undefined>();
  for (const rawLine of stdout.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const parts = line.split('\t');
    const id = (parts.length >= 2 ? parts[1] : parts[0])?.trim();
    const label = (parts.length >= 3 ? parts[2] : undefined)?.trim();
    if (!id || seen.has(id)) continue;
    seen.set(id, label || undefined);
  }
  return [...seen.entries()].map(([id, label]) => ({ id, ...(label ? { label } : {}) }));
}

function buildSubprocessEnv(requestEnv?: Record<string, string>): Record<string, string> {
  const baseEnv = Object.fromEntries(
    Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined)
  );
  return { ...baseEnv, ...(requestEnv ?? {}) };
}

function classifyAcpError(error: unknown): ProviderFailureClass {
  if (error instanceof ClassifiedProviderError) return error.failureClass;
  if (error instanceof AcpJsonRpcError && error.code === -32602) return 'misconfigured';
  return failureClassOfThrown(error);
}

export class AgyMultiProvider implements IAgentProvider {
  constructor(
    private readonly spawner?: AcpSpawner,
    private readonly processGroupOwner: AcpProcessGroupOwner = 'connection',
    private readonly runCli: (
      binaryPath: string,
      args: string[],
      options: { env: Record<string, string>; signal?: AbortSignal; timeout: number }
    ) => Promise<{ stdout: string; stderr: string }> = async (binaryPath, args, options) => {
      const { stdout, stderr } = await execFileAsync(binaryPath, args, options);
      return { stdout: String(stdout), stderr: String(stderr) };
    }
  ) {}

  getType(): string {
    return 'agy-multi';
  }

  getCapabilities(): ProviderCapabilities {
    return AGY_MULTI_CAPABILITIES;
  }

  async checkCredential(
    request: Parameters<IAgentProvider['checkCredential']>[0]
  ): Promise<CredentialStatus> {
    request.signal.throwIfAborted();
    const assistantConfig = parseAgyMultiConfig(request.assistantConfig ?? {});
    const env = buildSubprocessEnv(request.env);
    let binaryPath: string;
    try {
      binaryPath = resolveAgyMultiBinaryPath(assistantConfig.agyMultiBinaryPath, env);
    } catch (error) {
      const hasExplicitPin = Boolean(
        env.AGY_MULTI_BIN_PATH?.trim() || assistantConfig.agyMultiBinaryPath?.trim()
      );
      if (hasExplicitPin) {
        return {
          state: 'unusable',
          source: 'native',
          evidence: error instanceof Error ? error.message : String(error),
        };
      }
      return { state: 'not_connected', source: 'native' };
    }

    try {
      const { stdout } = await this.runCli(binaryPath, ['profile', 'list'], {
        env,
        signal: request.signal,
        timeout: SUBPROCESS_TIMEOUT_MS,
      });
      const summary = parseAgyMultiProfiles(stdout);
      if (summary.total === 0) {
        return { state: 'not_connected', source: 'native' };
      }
      if (summary.healthy === 0) {
        return {
          state: 'unusable',
          source: 'native',
          evidence: `All ${String(summary.total)} agy-multi profile(s) are disabled or unhealthy`,
        };
      }
      return { state: 'usable', source: 'native' };
    } catch (error) {
      request.signal.throwIfAborted();
      const secrets = collectCredentialValues(env);
      return {
        state: 'check_failed',
        source: 'native',
        evidence: redactCredentialValues(
          error instanceof Error ? error.message : String(error),
          secrets
        ),
      };
    }
  }

  async diagnose(
    request: Parameters<NonNullable<IAgentProvider['diagnose']>>[0]
  ): Promise<ProviderDiagnostics> {
    request.signal.throwIfAborted();
    const assistantConfig = parseAgyMultiConfig(request.assistantConfig ?? {});
    const env = buildSubprocessEnv();
    const secrets = collectCredentialValues(env);

    let binary: { path: string; source: string };
    try {
      binary = resolveAgyMultiBinaryWithSource(assistantConfig.agyMultiBinaryPath, env);
    } catch (error) {
      return {
        checks: [
          {
            id: 'binary',
            label: 'agy-multi CLI',
            status: 'fail',
            message: redactCredentialValues(
              error instanceof Error ? error.message : String(error),
              secrets
            ),
            remedy: 'Install agy-multi or set AGY_MULTI_BIN_PATH',
          },
        ],
      };
    }

    let versionText: string;
    try {
      const { stdout } = await this.runCli(binary.path, ['--version'], {
        env,
        signal: request.signal,
        timeout: SUBPROCESS_TIMEOUT_MS,
      });
      versionText = stdout.trim() || 'ok';
    } catch (error) {
      request.signal.throwIfAborted();
      return {
        checks: [
          {
            id: 'binary',
            label: 'agy-multi CLI',
            status: 'fail',
            message: `${binary.path} could not spawn: ${redactCredentialValues(
              error instanceof Error ? error.message : String(error),
              secrets
            )}`,
            remedy: 'Verify the agy-multi executable runs cleanly',
          },
        ],
      };
    }

    const checks: ProviderDiagnostics['checks'] = [
      {
        id: 'binary',
        label: 'agy-multi CLI',
        status: 'ok',
        message: `${binary.path} (${binary.source}, v${versionText})`,
      },
    ];

    try {
      const { stdout } = await this.runCli(binary.path, ['profile', 'list'], {
        env,
        signal: request.signal,
        timeout: SUBPROCESS_TIMEOUT_MS,
      });
      const summary = parseAgyMultiProfiles(stdout);
      if (summary.total === 0) {
        checks.push({
          id: 'profiles',
          label: 'agy-multi profiles',
          status: 'fail',
          message: 'No agy-multi profiles configured',
          remedy: 'Run: agy-multi profile add',
        });
      } else if (summary.healthy === 0) {
        checks.push({
          id: 'profiles',
          label: 'agy-multi profiles',
          status: 'fail',
          message: `${String(summary.total)} profile(s) configured, 0 healthy`,
          remedy: 'Run: agy-multi profile sync',
        });
      } else {
        checks.push({
          id: 'profiles',
          label: 'agy-multi profiles',
          status: 'ok',
          message: `${String(summary.healthy)}/${String(summary.total)} profile(s) healthy`,
        });
      }
    } catch (error) {
      request.signal.throwIfAborted();
      checks.push({
        id: 'profiles',
        label: 'agy-multi profiles',
        status: 'warn',
        message: `Could not inspect profiles: ${redactCredentialValues(
          error instanceof Error ? error.message : String(error),
          secrets
        )}`,
      });
    }

    return { checks };
  }

  async listModels(
    request: Parameters<NonNullable<IAgentProvider['listModels']>>[0]
  ): Promise<ProviderModelList> {
    request.signal.throwIfAborted();
    const env = buildSubprocessEnv();
    const binaryPath = resolveAgyMultiBinaryPath(undefined, env);
    const { stdout } = await this.runCli(binaryPath, ['models'], {
      env,
      signal: request.signal,
      timeout: SUBPROCESS_TIMEOUT_MS,
    });
    return { models: parseAgyMultiModels(stdout) };
  }

  async *sendQuery(
    prompt: string,
    cwd: string,
    resumeSessionId?: string,
    requestOptions?: SendQueryOptions
  ): AsyncGenerator<MessageChunk> {
    if (requestOptions?.abortSignal?.aborted) {
      throw new Error('Query aborted');
    }

    const assistantConfig = parseAgyMultiConfig(requestOptions?.assistantConfig ?? {});
    const env = buildSubprocessEnv(requestOptions?.env);
    let client: AgyMultiAcpClient | undefined;
    let resultReported = false;

    try {
      const binaryPath = resolveAgyMultiBinaryPath(assistantConfig.agyMultiBinaryPath, env);
      const args = buildAgyMultiAcpArgs(assistantConfig, requestOptions);
      client = new AgyMultiAcpClient({
        binaryPath,
        args,
        env,
        protectedEnvKeys: requestOptions?.protectedEnvKeys,
        spawner: this.spawner,
        processGroupOwner: this.processGroupOwner,
      });

      for await (const chunk of closeOpenToolCalls(
        streamAgyMultiTurn(client, prompt, cwd, resumeSessionId, assistantConfig, requestOptions),
        { resultEndsTurn: true }
      )) {
        if (chunk.type === 'result') resultReported = true;
        yield chunk;
      }
    } catch (error) {
      if (requestOptions?.abortSignal?.aborted) throw error;
      if (resultReported) {
        getLog().error({ err: error }, 'agy_multi.error_after_result');
      } else {
        const evidence =
          error instanceof AcpConnectionClosedError
            ? error.evidence
            : error instanceof Error
              ? error.message
              : String(error);
        const subtype =
          error instanceof AcpJsonRpcError ? 'agy_multi_request_failed' : 'agy_multi_query_failed';
        yield failureResult(classifyAcpError(error), subtype, evidence);
      }
    } finally {
      await client?.close();
    }

    if (requestOptions?.abortSignal?.aborted) {
      throw new Error('Query aborted');
    }
    yield { type: 'settled' };
  }
}
