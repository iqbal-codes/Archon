/**
 * agy-multi binary resolver.
 *
 * Resolution order:
 * 1. `AGY_MULTI_BIN_PATH` environment variable (request env, then process.env)
 * 2. `assistants['agy-multi'].agyMultiBinaryPath` in config
 * 3. `PATH` lookup + canonical install directories (`~/.local/bin/agy-multi`, etc.)
 * 4. Throw `ClassifiedProviderError('misconfigured', ...)` with install instructions
 */
import { homedir } from 'node:os';
import { delimiter, join } from 'node:path';
import { createLogger } from '@archon/paths';
import { envPathKey } from '@archon/paths/env-path';
import {
  appendBinaryCandidateHint,
  classifyBinaryPath,
  type BinaryPathKind,
} from '../shared/binary-resolution';
import { ClassifiedProviderError } from '../shared/failure';

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('agy-multi-binary');
  return cachedLog;
}

export const AGY_MULTI_BINARY_NAME = process.platform === 'win32' ? 'agy-multi.exe' : 'agy-multi';

export type AgyMultiBinarySource = 'env' | 'config' | 'path' | 'autodetect';

export interface AgyMultiBinaryResolution {
  path: string;
  source: AgyMultiBinarySource;
}

interface AgyMultiBinaryPin {
  sourceLabel: string;
  removableSetting: string;
  missingInstruction: string;
}

const AGY_MULTI_ENV_PIN: AgyMultiBinaryPin = {
  sourceLabel: 'AGY_MULTI_BIN_PATH',
  removableSetting: 'AGY_MULTI_BIN_PATH',
  missingInstruction: 'Verify the path points to the agy-multi executable.',
};

const AGY_MULTI_CONFIG_PIN: AgyMultiBinaryPin = {
  sourceLabel: "assistants['agy-multi'].agyMultiBinaryPath",
  removableSetting: 'agyMultiBinaryPath',
  missingInstruction: 'Verify the path in .archon/config.yaml points to the agy-multi executable.',
};

export function pathKind(path: string): BinaryPathKind {
  return classifyBinaryPath(path, error => {
    getLog().warn({ err: error, path, code: error.code }, 'agy_multi.path_stat_failed');
  });
}

function getPathDirs(env: Record<string, string | undefined>): string[] {
  const pathKey = envPathKey(env);
  const raw = env[pathKey];
  if (!raw) return [];
  return raw
    .split(delimiter)
    .map(entry => entry.trim())
    .filter(entry => entry.length > 0);
}

function getAutodetectPaths(): string[] {
  const home = homedir();
  if (process.platform === 'win32') {
    return [
      join(home, '.local', 'bin', AGY_MULTI_BINARY_NAME),
      join(home, 'AppData', 'Local', 'Programs', 'agy-multi', AGY_MULTI_BINARY_NAME),
    ];
  }
  return [
    join(home, '.local', 'bin', AGY_MULTI_BINARY_NAME),
    join('/usr', 'local', 'bin', AGY_MULTI_BINARY_NAME),
    join('/opt', 'homebrew', 'bin', AGY_MULTI_BINARY_NAME),
  ];
}

function findUnpinnedBinary(
  env: Record<string, string | undefined>
): AgyMultiBinaryResolution | undefined {
  for (const dir of getPathDirs(env)) {
    const candidate = join(dir, AGY_MULTI_BINARY_NAME);
    if (pathKind(candidate) === 'file') {
      return { path: candidate, source: 'path' };
    }
  }
  for (const candidate of getAutodetectPaths()) {
    if (pathKind(candidate) === 'file') {
      return { path: candidate, source: 'autodetect' };
    }
  }
  return undefined;
}

function validateAndExpand(
  rawPath: string,
  pin: AgyMultiBinaryPin,
  env: Record<string, string | undefined>
): string {
  const kind = pathKind(rawPath);
  if (kind === 'file') return rawPath;

  let message: string;
  if (kind === 'directory') {
    const candidate = join(rawPath, AGY_MULTI_BINARY_NAME);
    if (pathKind(candidate) === 'file') return candidate;
    message =
      `${pin.sourceLabel} is set to "${rawPath}", which is a directory, but it does not contain ${AGY_MULTI_BINARY_NAME}.\n` +
      'Point this setting at the agy-multi executable itself.';
  } else {
    message =
      `${pin.sourceLabel} is set to "${rawPath}" but the file does not exist.\n` +
      pin.missingInstruction;
  }

  const candidate = findUnpinnedBinary(env);
  throw new ClassifiedProviderError(
    'misconfigured',
    appendBinaryCandidateHint(message, {
      candidatePath: candidate?.path,
      binaryLabel: 'agy-multi',
      sourceLabel: pin.sourceLabel,
      removableSetting: pin.removableSetting,
    })
  );
}

const INSTALL_INSTRUCTIONS =
  'agy-multi CLI not found. Install agy-multi and add an account profile (`agy-multi profile add`),\n' +
  'or set AGY_MULTI_BIN_PATH / assistants.agy-multi.agyMultiBinaryPath to its executable path.';

export function resolveAgyMultiBinaryWithSource(
  configBinaryPath?: string,
  requestEnv?: Record<string, string | undefined>
): AgyMultiBinaryResolution {
  const env: Record<string, string | undefined> = { ...process.env, ...(requestEnv ?? {}) };

  const envPath = env.AGY_MULTI_BIN_PATH?.trim();
  if (envPath) {
    return {
      path: validateAndExpand(envPath, AGY_MULTI_ENV_PIN, env),
      source: 'env',
    };
  }

  const configPath = configBinaryPath?.trim();
  if (configPath) {
    return {
      path: validateAndExpand(configPath, AGY_MULTI_CONFIG_PIN, env),
      source: 'config',
    };
  }

  const detected = findUnpinnedBinary(env);
  if (detected) return detected;

  throw new ClassifiedProviderError('misconfigured', INSTALL_INSTRUCTIONS);
}

export function resolveAgyMultiBinaryPath(
  configBinaryPath?: string,
  requestEnv?: Record<string, string | undefined>
): string {
  return resolveAgyMultiBinaryWithSource(configBinaryPath, requestEnv).path;
}
