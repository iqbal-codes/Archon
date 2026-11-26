import { describe, expect, test } from 'bun:test';
import { InvalidProviderRunConfigError } from '@archon/provider-contract';
import { parseAgyMultiConfig, parseAgyMultiConfigStrict } from './config';

describe('agy-multi config', () => {
  test('strict install/run/snapshot parsing validates fields and strips local fields on snapshot', () => {
    const raw = {
      model: ' gemini-4-hydrogen-high ',
      effort: 'high',
      agyMultiBinaryPath: ' /usr/local/bin/agy-multi ',
      clean: false,
      compactionThreshold: 250_000,
      switchDelay: 5,
      handoffContext: 4096,
      additionalDirectories: ['/tmp/extra'],
    };

    const parsed = parseAgyMultiConfigStrict(raw, 'install');
    expect(parsed.model).toBe('gemini-4-hydrogen-high');
    expect(parsed.agyMultiBinaryPath).toBe('/usr/local/bin/agy-multi');
    expect(parsed.effort).toBe('high');

    const snapshot = parseAgyMultiConfigStrict(raw, 'snapshot');
    expect(snapshot).toEqual({
      model: 'gemini-4-hydrogen-high',
      effort: 'high',
    });
  });

  test('strict parsing rejects unknown keys and invalid field values', () => {
    expect(() => parseAgyMultiConfigStrict({ unknownKey: 1 })).toThrow(
      InvalidProviderRunConfigError
    );
    expect(() => parseAgyMultiConfigStrict({ effort: 'turbo' })).toThrow(
      InvalidProviderRunConfigError
    );
    expect(() => parseAgyMultiConfigStrict({ model: '   ' })).toThrow(
      InvalidProviderRunConfigError
    );
  });

  test('lenient parsing drops invalid keys while preserving valid ones', () => {
    const parsed = parseAgyMultiConfig({
      model: 'gemini-3.8-flash-low',
      effort: 'bogus',
      unknownKey: 'drop-me',
      clean: true,
    });
    expect(parsed).toEqual({
      model: 'gemini-3.8-flash-low',
      clean: true,
    });
  });
});
