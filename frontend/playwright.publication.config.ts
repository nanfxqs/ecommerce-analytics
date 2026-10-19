import { defineConfig } from '@playwright/test';
export default defineConfig({ testDir: './tests', testMatch: 'publication.spec.ts', timeout: 60000, use: { baseURL: 'http://127.0.0.1:5178', launchOptions: { executablePath: '/usr/bin/chromium', args: ['--no-sandbox'] } }, webServer: [
  { command: 'RETAILROCKET_API=http://127.0.0.1:8005 npm run dev -- --port 5178', url: 'http://127.0.0.1:5178', reuseExistingServer: false }
] });
