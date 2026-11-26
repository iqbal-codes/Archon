import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { removeTempTree } from '@archon/paths/test-utils';
import { ClassifiedProviderError } from '../shared/failure';
import {
  AGY_MULTI_BINARY_NAME,
  resolveAgyMultiBinaryPath,
  resolveAgyMultiBinaryWithSource,
} from './binary-resolver';

describe('agy-multi binary resolver', () => {
  let root: string;
  let savedBinPath: string | undefined;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'archon-agy-multi-bin-'));
    savedBinPath = process.env.AGY_MULTI_BIN_PATH;
    delete process.env.AGY_MULTI_BIN_PATH;
  });

  afterEach(async () => {
    if (savedBinPath === undefined) delete process.env.AGY_MULTI_BIN_PATH;
    else process.env.AGY_MULTI_BIN_PATH = savedBinPath;
    await removeTempTree(root);
  });

  test('prefers AGY_MULTI_BIN_PATH over config and PATH', () => {
    const envBin = join(root, 'env-bin');
    const configBin = join(root, 'config-bin');
    writeFileSync(envBin, '#!/bin/sh\n');
    writeFileSync(configBin, '#!/bin/sh\n');

    const resolved = resolveAgyMultiBinaryWithSource(configBin, {
      AGY_MULTI_BIN_PATH: envBin,
      PATH: '',
    });
    expect(resolved).toEqual({ path: envBin, source: 'env' });
  });

  test('resolves config pin and expands directory pins containing agy-multi', () => {
    const dir = join(root, 'bin');
    mkdirSync(dir, { recursive: true });
    const bin = join(dir, AGY_MULTI_BINARY_NAME);
    writeFileSync(bin, '#!/bin/sh\n');

    expect(resolveAgyMultiBinaryWithSource(dir, { PATH: '' })).toEqual({
      path: bin,
      source: 'config',
    });
  });

  test('resolves agy-multi from PATH when unpinned', () => {
    const dir = join(root, 'path-bin');
    mkdirSync(dir, { recursive: true });
    const bin = join(dir, AGY_MULTI_BINARY_NAME);
    writeFileSync(bin, '#!/bin/sh\n');

    expect(resolveAgyMultiBinaryWithSource(undefined, { PATH: dir })).toEqual({
      path: bin,
      source: 'path',
    });
  });

  test('throws misconfigured ClassifiedProviderError when an explicit pin does not exist', () => {
    const missing = join(root, 'does-not-exist');
    let caught: unknown;
    try {
      resolveAgyMultiBinaryPath(missing, { PATH: '' });
    } catch (error) {
      caught = error;
    }
    expect(caught).toBeInstanceOf(ClassifiedProviderError);
    expect((caught as ClassifiedProviderError).failureClass).toBe('misconfigured');
    expect((caught as Error).message).toContain(missing);
  });
});
