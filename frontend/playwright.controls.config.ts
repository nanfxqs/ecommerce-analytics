import { defineConfig } from '@playwright/test';
export default defineConfig({ testDir: './tests', testMatch: 'replay-controls.spec.ts', timeout: 60000, use: { baseURL: 'http://127.0.0.1:5174', launchOptions: { executablePath: '/usr/bin/chromium', args: ['--no-sandbox'] } }, webServer: [
{command:'RETAILROCKET_API=http://127.0.0.1:8001 npm run dev -- --port 5174',url:'http://127.0.0.1:5174',reuseExistingServer:false}
]});
