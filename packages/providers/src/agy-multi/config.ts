/**
 * agy-multi provider config: the schema owns the shape, and the defaults type is
 * derived from it.
 */
import {
  EFFORT_LADDER,
  snapshotConfigSchema,
  type ProviderConfigScope,
} from '@archon/provider-contract';
import { z } from 'zod';
import { configStringSchema, parseConfigLenient, parseConfigSchema } from '../shared/run-config';

/** Archon's shared effort ladder; clamped to agy-multi's `low | medium | high | xhigh` at runtime. */
export const AGY_MULTI_EFFORTS = EFFORT_LADDER;

const portableFields = {
  model: configStringSchema.optional(),
  effort: z
    .enum(AGY_MULTI_EFFORTS, { error: `expected ${AGY_MULTI_EFFORTS.join(', ')}` })
    .optional(),
};

const localFields = {
  /** Path to the agy-multi executable. Overrides auto-detection and AGY_MULTI_BIN_PATH. */
  agyMultiBinaryPath: configStringSchema.optional(),
  /** Suppress default customizations (--include-default-customizations=false) even in chat. */
  clean: z.boolean().optional(),
  /** Auto-compaction context threshold passed to `agy-multi acp --compaction-threshold`. */
  compactionThreshold: z.number().int().nonnegative().optional(),
  /** Delay in seconds before handing a turn to another account (`--switch-delay`). */
  switchDelay: z.number().nonnegative().optional(),
  /** Max transcript chars replayed on fallback handoff (`--handoff-context`). */
  handoffContext: z.number().int().nonnegative().optional(),
  /** Additional workspace directories passed to `agy acp --add-dir`. */
  additionalDirectories: z.array(configStringSchema).optional(),
};

const fields = { ...portableFields, ...localFields };

export const configSchemas = {
  install: z.strictObject(fields),
  run: z.strictObject(fields),
  snapshot: snapshotConfigSchema(portableFields, localFields),
};

export type AgyMultiProviderDefaults = z.output<typeof configSchemas.install>;

/** Parse raw assistantConfig into typed agy-multi defaults, dropping invalid values. */
export function parseAgyMultiConfig(raw: Record<string, unknown>): AgyMultiProviderDefaults {
  return parseConfigLenient(configSchemas.install, raw, 'agy-multi');
}

export function parseAgyMultiConfigStrict(
  raw: Record<string, unknown>,
  scope: ProviderConfigScope = 'install'
): AgyMultiProviderDefaults {
  return parseConfigSchema(configSchemas[scope], raw);
}
