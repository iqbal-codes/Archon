import type { ProviderCapabilities } from '../types';

/**
 * agy-multi ACP provider capabilities.
 *
 * Capabilities reflect only behavior wired and enforced over `agy-multi acp`:
 * - `sessionResume` & `sessionFork` use ACP `session/resume` and `session/fork`.
 * - `effortControl` maps Archon's effort ladder onto ACP `thought_level`.
 * - `structuredOutput` uses best-effort prompt augmentation + JSON repair/validation.
 * - `mcp` and `skills` remain false until `agy acp` exposes strict per-node isolation
 *   for user MCP servers and named skill directories.
 */
export const AGY_MULTI_CAPABILITIES: ProviderCapabilities = {
  backgroundWork: 'unobserved',
  sessionResume: true,
  sessionFork: true,
  mcp: false,
  hooks: false,
  skills: false,
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
