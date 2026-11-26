export const DEFAULT_CHAT_MODEL_OPTIONS: Record<string, { value: string; hint?: string }[]> = {
  claude: [
    { value: 'sonnet', hint: 'balanced (SDK default)' },
    { value: 'opus', hint: 'most capable' },
    { value: 'haiku', hint: 'fastest' },
  ],
  codex: [{ value: 'gpt-6.1-sol' }, { value: 'gpt-6.1-terra' }, { value: 'gpt-6-luna' }],
  'agy-multi': [
    { value: 'gemini-4-hydrogen-high', hint: 'most capable (default)' },
    { value: 'gemini-4-hydrogen-medium', hint: 'balanced' },
    { value: 'gemini-3.8-flash-low', hint: 'fastest' },
  ],
};
