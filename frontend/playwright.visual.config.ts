import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './tests', testMatch: ['history.spec.ts', 'history-pages.spec.ts', 'visualizations.spec.ts'],
  use: { baseURL: 'http://127.0.0.1:5181', launchOptions: { executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', args: ['--no-sandbox'] } },
  webServer: [
    { command: '../.venv/bin/python -m retailrocket.api --store ../.scratch/browser-history/published --port 8011', url: 'http://127.0.0.1:8011/api/history/overview', reuseExistingServer: false, timeout: 30000 },
    { command: 'RETAILROCKET_API=http://127.0.0.1:8011 npm run dev -- --port 5181 --strictPort', url: 'http://127.0.0.1:5181', reuseExistingServer: false, timeout: 30000 },
  ],
});
