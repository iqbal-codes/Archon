import { PROVIDER_PLUGIN_PROTOCOL } from '@archon/provider-contract/plugin/wire';
import { z } from 'zod';
import { BUNDLED_VERSION } from '@archon/paths';
import { providerPluginDescriptorSchema } from '@archon/provider-contract/plugin/wire';
import { AGY_MULTI_CAPABILITIES } from './capabilities';
import { configSchemas } from './config';

export const descriptor = providerPluginDescriptorSchema.parse({
  protocol: PROVIDER_PLUGIN_PROTOCOL,
  id: 'agy-multi',
  displayName: 'Antigravity (agy-multi)',
  version: BUNDLED_VERSION,
  capabilities: AGY_MULTI_CAPABILITIES,
  credentials: { kind: 'static', specs: [] },
  configSchema: z.toJSONSchema(configSchemas.install, { io: 'input' }),
  config: {
    install: z.toJSONSchema(configSchemas.install, { io: 'input' }),
    run: z.toJSONSchema(configSchemas.run, { io: 'input' }),
    snapshot: z.toJSONSchema(configSchemas.snapshot, { io: 'input' }),
    stripUnknownKeys: true,
  },
});
