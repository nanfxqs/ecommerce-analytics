import { defineConfig } from '@playwright/test';
export default defineConfig({ testDir: './tests', use: { baseURL: 'http://127.0.0.1:5173', launchOptions: { executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', args: ['--no-sandbox'] } }, webServer: [
  { command: '../.venv/bin/python -m retailrocket.api --store ../.scratch/browser-history/published', url: 'http://127.0.0.1:8000/api/history/overview', reuseExistingServer: false, timeout: 30000 },
  { command: 'npm run dev', url: 'http://127.0.0.1:5173', reuseExistingServer: false, timeout: 30000 }
] });
