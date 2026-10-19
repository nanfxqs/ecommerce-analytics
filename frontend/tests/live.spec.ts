import { test, expect } from '@playwright/test';
test.skip(!process.env.LIVE_METRICS, 'Requires actual #14 known-input YARN smoke and live API');
test('committed realtime business and quality results survive browser reload', async ({ page }) => {
  await page.goto('/');
  await page.getByText('实时监控', { exact: true }).click();
  await expect(page.getByText('本轮持续分析 · 已提交结果', { exact: true })).toBeVisible();
  // Known61-row immutable smoke input, independently checked against source.
  await expect(page.getByTestId('live-valid')).toHaveText('61');
  await expect(page.getByTestId('live-visitors')).toHaveText('60');
  await expect(page.getByTestId('live-view')).toHaveText('59');
  await expect(page.getByTestId('live-cart')).toHaveText('2');
  await expect(page.getByTestId('live-purchase')).toHaveText('0');
  await expect(page.getByTestId('live-invalid_count')).toHaveText('0');
  await expect(page.getByTestId('live-retry_count')).toHaveText('0');
  await expect(page.getByRole('img', { name: '本轮原始事件 UTC 分日趋势' })).toBeVisible();
  await expect(page.getByText('结果接收时间（本机 UTC）', { exact: false })).toBeVisible();
  await expect(page.getByText('2015-06-02', { exact: true }).first()).toBeVisible();
  await page.reload();
  await page.getByText('实时监控', { exact: true }).click();
  await expect(page.getByTestId('live-valid')).toHaveText('61');
  await expect(page.getByTestId('live-visitors')).toHaveText('60');
  await page.screenshot({ path: '/tmp/ecommerce-v2-agent/live-browser.png', fullPage: true });
});
