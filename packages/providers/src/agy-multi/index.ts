import { AgyMultiProvider } from './provider';

export { AgyMultiProvider } from './provider';
export { parseAgyMultiConfig, type AgyMultiProviderDefaults } from './config';
export { resolveAgyMultiBinaryPath } from './binary-resolver';
export { descriptor } from './descriptor';

export function createProvider(
  ...args: ConstructorParameters<typeof AgyMultiProvider>
): AgyMultiProvider {
  return new AgyMultiProvider(...args);
}
