import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globalSetup: ['./vitest.global-setup.ts'],
    // Conformance tests each boot an engine subprocess + pglite; keep memory bounded.
    pool: 'forks',
    poolOptions: { forks: { maxForks: 4 } },
    testTimeout: 60000,
    hookTimeout: 60000,
  },
})
