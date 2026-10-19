import { defineConfig } from '@playwright/test';
export default defineConfig({ testDir: './tests', testMatch: 'acceptance-full.spec.ts', timeout: 120000, use: { baseURL: 'http://127.0.0.1:5178', launchOptions: { executablePath: '/usr/bin/chromium', args: ['--no-sandbox'] } }, webServer: [
  { command: 'RETAILROCKET_API=http://127.0.0.1:8006 npm run dev -- --port 5178', url: 'http://127.0.0.1:5178', reuseExistingServer: false }
] });
