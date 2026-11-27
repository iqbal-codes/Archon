import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { PassThrough } from 'node:stream';
import { tmpdir } from 'node:os';
import type { ChildProcessWithoutNullStreams } from 'node:child_process';
import { runProviderConformance } from '@archon/provider-contract/conformance';
import { removeTempTree } from '@archon/paths/test-utils';
import { AgyMultiProvider } from './provider';
import type { AcpSpawner } from './acp-client';

function createFakeSpawner(
  recordedArgs: string[][] = [],
  recordedRequests: { method: string; params?: Record<string, unknown> }[] = []
): AcpSpawner {
  return (_command, args) => {
    recordedArgs.push([...args]);

    const child = new EventEmitter() as ChildProcessWithoutNullStreams;
    const stdin = new PassThrough();
    const stdout = new PassThrough();
    const stderr = new PassThrough();
    Object.assign(child, {
      stdin,
      stdout,
      stderr,
      pid: 99999,
      exitCode: null,
      signalCode: null,
    });

    const send = (payload: Record<string, unknown>): void => {
      stdout.write(`${JSON.stringify(payload)}\n`);
    };

    let mode: 'basic' | 'tools' | 'fork' | 'structured' = 'basic';
    let sessionId = 'sess-1';
    let buffer = '';

    stdin.on('data', (chunk: Buffer) => {
      buffer += chunk.toString('utf8');
      while (true) {
        const idx = buffer.indexOf('\n');
        if (idx === -1) break;
        const line = buffer.slice(0, idx).trim();
        buffer = buffer.slice(idx + 1);
        if (!line) continue;
        const msg = JSON.parse(line) as {
          id?: number | string;
          method?: string;
          params?: Record<string, unknown>;
        };
        if (!msg.method || msg.id === undefined) continue;
        recordedRequests.push({ method: msg.method, params: msg.params });

        if (msg.method === 'initialize') {
          send({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: 1 } });
        } else if (msg.method === 'session/fork') {
          mode = 'fork';
          sessionId = 'sess-forked';
          send({ jsonrpc: '2.0', id: msg.id, result: { sessionId } });
        } else if (msg.method === 'session/new') {
          sessionId = 'sess-1';
          send({
            jsonrpc: '2.0',
            id: msg.id,
            result: {
              sessionId,
              configOptions: [
                {
                  id: 'thought_level',
                  options: [{ value: 'low' }, { value: 'medium' }, { value: 'high' }],
                },
              ],
            },
          });
        } else if (msg.method === 'session/set_model') {
          if (msg.params?.modelId === 'bad-model') {
            send({
              jsonrpc: '2.0',
              id: msg.id,
              error: { code: -32602, message: 'invalid model: bad-model' },
            });
          } else {
            send({ jsonrpc: '2.0', id: msg.id, result: {} });
          }
        } else if (msg.method === 'session/set_config_option') {
          send({ jsonrpc: '2.0', id: msg.id, result: {} });
        } else if (msg.method === 'session/prompt') {
          const promptArr = msg.params?.prompt as { text?: string }[] | undefined;
          const promptText = promptArr?.[0]?.text ?? '';
          if (promptText.includes('run tool')) mode = 'tools';
          else if (promptText.includes('analyze')) mode = 'structured';

          if (mode === 'tools') {
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'tool_call',
                  toolCallId: 'call-1',
                  name: 'run_command',
                  title: 'echo hi',
                  rawInput: { CommandLine: 'echo hi' },
                  status: 'in_progress',
                },
              },
            });
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'tool_call_update',
                  toolCallId: 'call-1',
                  status: 'in_progress',
                },
              },
            });
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'tool_call_update',
                  toolCallId: 'call-1',
                  status: 'completed',
                  rawOutput: { output: 'hi\n', exit_code: 0 },
                },
              },
            });
            // Leave call-2 open so closeOpenToolCalls closes it as cancelled.
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'tool_call',
                  toolCallId: 'call-2',
                  name: 'read_file',
                  status: 'in_progress',
                },
              },
            });
            send({ jsonrpc: '2.0', id: msg.id, result: { stopReason: 'end_turn' } });
          } else if (mode === 'structured') {
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'agent_message_chunk',
                  content: { type: 'text', text: '```json\n{"ok": true}\n```' },
                },
              },
            });
            send({ jsonrpc: '2.0', id: msg.id, result: { stopReason: 'end_turn' } });
          } else {
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'agent_thought_chunk',
                  content: { type: 'text', text: 'thinking' },
                },
              },
            });
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'agent_message_chunk',
                  content: { type: 'text', text: mode === 'fork' ? 'forked ' : 'Hello ' },
                },
              },
            });
            send({
              jsonrpc: '2.0',
              method: 'session/update',
              params: {
                sessionId,
                update: {
                  sessionUpdate: 'agent_message_chunk',
                  content: { type: 'text', text: 'world' },
                },
              },
            });
            send({
              jsonrpc: '2.0',
              id: msg.id,
              result: {
                stopReason: 'end_turn',
                usage: { inputTokens: 10, outputTokens: 2 },
              },
            });
          }
        }
      }
    });

    stdin.on('end', () => {
      stdout.end();
      stderr.end();
      (child as { exitCode: number | null }).exitCode = 0;
      child.emit('close', 0, null);
    });

    return child;
  };
}

describe('AgyMultiProvider', () => {
  let root: string;
  let fakeBinary: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'archon-agy-multi-provider-'));
    fakeBinary = join(root, 'agy-multi');
    writeFileSync(fakeBinary, '#!/bin/sh\n');
  });

  afterEach(async () => {
    await removeTempTree(root);
  });

  test('passes conformance suite for turns, tools, resume, fork, and failure cases', async () => {
    const provider = new AgyMultiProvider(createFakeSpawner());

    const failures = await runProviderConformance({
      capabilities: provider.getCapabilities(),
      turns: [
        {
          name: 'basic-turn',
          run: () =>
            provider.sendQuery('hello', root, undefined, {
              assistantConfig: { agyMultiBinaryPath: fakeBinary },
            }),
        },
      ],
      toolTurn: {
        name: 'tool-turn',
        run: () =>
          provider.sendQuery('run tool', root, undefined, {
            assistantConfig: { agyMultiBinaryPath: fakeBinary },
          }),
      },
      forkTurn: {
        name: 'fork-turn',
        source: 'sess-1',
        run: () =>
          provider.sendQuery('fork', root, 'sess-1', {
            forkSession: true,
            assistantConfig: { agyMultiBinaryPath: fakeBinary },
          }),
      },
      failureCases: [
        {
          name: 'invalid-model',
          expected: 'misconfigured',
          evidence: 'invalid model: bad-model',
          run: () =>
            provider.sendQuery('fail', root, undefined, {
              model: 'bad-model',
              assistantConfig: { agyMultiBinaryPath: fakeBinary },
            }),
        },
      ],
    });
    expect(failures).toEqual([]);
  });

  test('disables default customizations on workflow nodes and forwards model, effort, and structured output', async () => {
    const recordedArgs: string[][] = [];
    const requests: { method: string; params?: Record<string, unknown> }[] = [];
    const provider = new AgyMultiProvider(createFakeSpawner(recordedArgs, requests));

    const chunks = await Array.fromAsync(
      provider.sendQuery('analyze', root, undefined, {
        model: 'gemini-3.8-flash-low',
        appendSystemPrompt: 'Be concise.',
        outputFormat: {
          type: 'json_schema',
          schema: { type: 'object', properties: { ok: { type: 'boolean' } }, required: ['ok'] },
        },
        nodeConfig: {
          nodeId: 'plan',
          effort: 'persistent',
        },
        assistantConfig: {
          agyMultiBinaryPath: fakeBinary,
        },
      })
    );

    expect(recordedArgs[0]).not.toContain('--sequential');
    expect(recordedArgs[0]).toContain('--clean');
    expect(recordedArgs[0]).toContain('--dangerously-skip-permissions');
    expect(recordedArgs[0]).toContain('--include-default-customizations=false');

    const newSession = requests.find(r => r.method === 'session/new');
    expect(newSession?.params?._meta).toEqual({
      antigravity: { systemPrompt: { append: 'Be concise.' } },
    });
    expect(requests.find(r => r.method === 'session/set_model')?.params).toEqual({
      sessionId: 'sess-1',
      modelId: 'gemini-3.8-flash-low',
    });
    // persistent -> xhigh -> clamped to high because model only advertises low/medium/high
    expect(requests.find(r => r.method === 'session/set_config_option')?.params).toEqual({
      sessionId: 'sess-1',
      configId: 'thought_level',
      value: 'high',
    });

    const resultChunk = chunks.find(c => c.type === 'result');
    expect(resultChunk).toMatchObject({
      type: 'result',
      sessionId: 'sess-1',
      structuredOutput: { ok: true },
    });
    expect(chunks.at(-1)).toEqual({ type: 'settled' });
  });

  test('checkCredential, diagnose, and listModels interpret agy-multi CLI output', async () => {
    const provider = new AgyMultiProvider(undefined, 'connection', async (_bin, args) => {
      if (args[0] === '--version') return { stdout: '1.3.1\n', stderr: '' };
      if (args[0] === 'profile') {
        return {
          stdout: [
            'account-1\ta@example.com\tok\tlastOk=2026-11-25T00:00:00Z',
            'account-2\tb@example.com\tdisabled\t',
          ].join('\n'),
          stderr: '',
        };
      }
      if (args[0] === 'models') {
        return {
          stdout: [
            'account-1\tgemini-4-hydrogen-high\tGemini 4 Hydrogen (High)',
            'account-2\tgemini-4-hydrogen-high\tGemini 4 Hydrogen (High)',
            'account-1\tgemini-3.8-flash-low\tGemini 3.8 Flash (Low)',
          ].join('\n'),
          stderr: '',
        };
      }
      return { stdout: '', stderr: '' };
    });

    const cred = await provider.checkCredential({
      env: {},
      assistantConfig: { agyMultiBinaryPath: fakeBinary },
      signal: AbortSignal.timeout(5000),
    });
    expect(cred).toEqual({ state: 'usable', source: 'native' });

    const diag = await provider.diagnose({
      assistantConfig: { agyMultiBinaryPath: fakeBinary },
      signal: AbortSignal.timeout(5000),
    });
    expect(diag.checks.map(c => ({ id: c.id, status: c.status }))).toEqual([
      { id: 'binary', status: 'ok' },
      { id: 'profiles', status: 'ok' },
    ]);
    // Never leak profile email addresses into diagnostics.
    expect(JSON.stringify(diag)).not.toContain('example.com');

    const originalEnv = process.env.AGY_MULTI_BIN_PATH;
    process.env.AGY_MULTI_BIN_PATH = fakeBinary;
    try {
      const models = await provider.listModels({ signal: AbortSignal.timeout(5000) });
      expect(models).toEqual({
        models: [
          { id: 'gemini-4-hydrogen-high', label: 'Gemini 4 Hydrogen (High)' },
          { id: 'gemini-3.8-flash-low', label: 'Gemini 3.8 Flash (Low)' },
        ],
      });
    } finally {
      if (originalEnv === undefined) delete process.env.AGY_MULTI_BIN_PATH;
      else process.env.AGY_MULTI_BIN_PATH = originalEnv;
    }
  });

  test('maps nodeConfig.mcp and nodeConfig.skills into ACP session/new with warnings for missing entries', async () => {
    const recordedArgs: string[][] = [];
    const requests: { method: string; params?: Record<string, unknown> }[] = [];
    const provider = new AgyMultiProvider(createFakeSpawner(recordedArgs, requests));

    const skillDir = join(root, '.agents', 'skills', 'code-check');
    mkdirSync(skillDir, { recursive: true });
    writeFileSync(join(skillDir, 'SKILL.md'), '---\nname: code-check\n---\nCheck code.');

    const mcpPath = join(root, '.archon', 'mcp.json');
    mkdirSync(join(root, '.archon'), { recursive: true });
    writeFileSync(
      mcpPath,
      JSON.stringify({
        mcpServers: {
          localTool: { command: 'node', args: ['tool.js'], env: { API_TOKEN: '${MY_API_TOKEN}' } },
          remoteHttp: {
            url: 'https://mcp.example.com/mcp',
            headers: { Authorization: 'Bearer ${MISSING_TOKEN}' },
          },
        },
      })
    );

    const chunks = await Array.fromAsync(
      provider.sendQuery('run checks', root, undefined, {
        appendSystemPrompt: 'Stay focused.',
        env: { MY_API_TOKEN: 'secret-123' },
        nodeConfig: {
          nodeId: 'review',
          mcp: '.archon/mcp.json',
          skills: ['code-check', 'nonexistent-skill'],
        },
        assistantConfig: { agyMultiBinaryPath: fakeBinary },
      })
    );

    const newSession = requests.find(r => r.method === 'session/new');
    expect(newSession?.params?.mcpServers).toEqual([
      {
        name: 'localTool',
        command: 'node',
        args: ['tool.js'],
        env: [{ name: 'API_TOKEN', value: 'secret-123' }],
      },
      {
        type: 'http',
        name: 'remoteHttp',
        url: 'https://mcp.example.com/mcp',
        headers: [{ name: 'Authorization', value: 'Bearer ' }],
      },
    ]);
    expect(newSession?.params?._meta).toEqual({
      antigravity: {
        systemPrompt: { append: 'Stay focused.' },
        skills: [skillDir],
      },
    });

    const warnings = chunks.filter(c => c.type === 'warning');
    expect(warnings).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: 'agy_multi.mcp_env_vars_missing' }),
        expect.objectContaining({ code: 'agy_multi.skills_missing' }),
      ])
    );
    expect(chunks.find(c => c.type === 'result')).toMatchObject({
      type: 'result',
      sessionId: 'sess-1',
    });
  });

  test('fails as misconfigured when nodeConfig.mcp references a missing file', async () => {
    const provider = new AgyMultiProvider(createFakeSpawner());
    const chunks = await Array.fromAsync(
      provider.sendQuery('hello', root, undefined, {
        nodeConfig: { nodeId: 'step', mcp: 'missing.mcp.json' },
        assistantConfig: { agyMultiBinaryPath: fakeBinary },
      })
    );
    expect(chunks.find(c => c.type === 'result')).toMatchObject({
      type: 'result',
      isError: true,
      failure: { class: 'misconfigured' },
    });
  });
});
