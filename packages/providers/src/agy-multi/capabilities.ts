import type { ProviderCapabilities } from '../types';

/**
 * agy-multi ACP provider capabilities.
 *
 * Capabilities reflect only behavior wired and enforced over `agy-multi acp`:
 * - `sessionResume` & `sessionFork` use ACP `session/resume` and `session/fork`.
 * - `mcp` loads `nodeConfig.mcp` via `loadMcpConfig` and maps it to ACP `session/new.mcpServers`,
 *   while `agy-multi acp --clean` isolates ambient user MCP configs via `--gemini-dir`.
 * - `skills` resolves `nodeConfig.skills` via `@archon/paths/skills` and passes them to
 *   `agy-multi acp` via `session/new._meta.antigravity.skills` (with `--clean` suppressing
 *   ambient user skills; upstream `agy acp` still discovers `<cwd>/.agents/skills` when present).
 * - `effortControl` maps Archon's effort ladder onto ACP `thought_level`.
 * - `structuredOutput` uses best-effort prompt augmentation + JSON repair/validation.
 */
export const AGY_MULTI_CAPABILITIES: ProviderCapabilities = {
  backgroundWork: 'unobserved',
  sessionResume: true,
  sessionFork: true,
  mcp: true,
  hooks: false,
  skills: true,
  plugins: false,
  agents: false,
  toolRestrictions: false,
  structuredOutput: 'best-effort',
  structuredOutputCorrection: 'fresh-session',
  requiresAllPropertiesRequired: false,
  envInjection: true,
  costControl: false,
  costReporting: false,
  tokenReporting: true,
  stopReasonReporting: true,
  turnCountReporting: false,
  resolvedModelReporting: false,
  effortControl: true,
  fallbackModel: false,
  sandbox: false,
  settingSources: false,
  nativeTools: false,
  containerExec: false,
};
