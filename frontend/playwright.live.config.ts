import { defineConfig } from '@playwright/test';
export default defineConfig({ testDir: './tests', testMatch: 'live.spec.ts', timeout: 60000, use: { baseURL: 'http://127.0.0.1:5176', launchOptions: { executablePath: '/usr/bin/chromium', args: ['--no-sandbox'] } }, webServer: [
  { command: 'RETAILROCKET_API=http://127.0.0.1:8003 npm run dev -- --port 5176', url: 'http://127.0.0.1:5176', reuseExistingServer: false }
] });
