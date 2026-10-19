import { test, expect } from '@playwright/test';
test.skip(!process.env.REPLAY_LIVE, 'Requires production smoke state and live API');
test('real HTTP replay monitor survives reload with committed progress', async ({ page }) => {
  await page.goto('/');
  await page.getByText('实时监控', { exact: true }).click();
  await expect(page.getByText('历史数据回放监控', { exact: true })).toBeVisible();
  await expect(page.getByText('回放处理完成', { exact: true })).toBeVisible({ timeout: 45000 });
  await expect(page.getByText('61', { exact: true })).toBeVisible();
  await expect(page.getByText('最近原始消息样本 · 清洗前 · 最多 20 条', { exact: true })).toBeVisible();
  await expect(page.getByText('Spark 已成功提交检查点', { exact: false })).toBeVisible();
  await page.reload();
  await page.getByText('实时监控', { exact: true }).click();
  await expect(page.getByText('回放处理完成', { exact: true })).toBeVisible({ timeout: 45000 });
});
