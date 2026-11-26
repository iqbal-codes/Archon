import { serveProvider } from '@archon/provider-contract/plugin';
import { installProviderLogSink } from '../shared/plugin-logging';
import type { AcpSpawner } from './acp-client';
import { descriptor } from './descriptor';
import { AgyMultiProvider } from './provider';

export { descriptor };

// The plugin host owns cancellation and cleanup of this process group, including child processes.
export function createProvider(spawner?: AcpSpawner): AgyMultiProvider {
  return new AgyMultiProvider(spawner, 'host');
}

if (import.meta.main) {
  await serveProvider({
    descriptor,
    create: log => {
      installProviderLogSink(log);
      return createProvider();
    },
  });
}
