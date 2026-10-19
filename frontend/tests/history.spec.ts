import { test, expect } from '@playwright/test';
test('known published history supports inclusive UTC filters and empty days', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByText('批次 known', { exact: false })).toBeVisible();
  await expect(page.getByTestId('count-view')).toHaveText('4');
  await expect(page.getByTestId('count-visitors')).toHaveText('4');
  await page.getByLabel('开始日期').fill('1970-01-01');
  await page.getByLabel('结束日期').fill('1970-01-02');
  await page.getByRole('button', { name: '应用日期' }).click();
  await expect(page.getByTestId('count-view')).toHaveText('3');
  await expect(page.getByTestId('count-visitors')).toHaveText('3');
  await expect(page.getByRole('img', { name: 'UTC 每日行为与独立访客趋势' })).toBeVisible();
  await page.screenshot({ path: '../.scratch/history-overview.png', fullPage: true });
  await page.getByLabel('开始日期').fill('1970-01-03');
  await page.getByLabel('结束日期').fill('1970-01-03');
  await page.getByRole('button', { name: '应用日期' }).click();
  await expect(page.getByTestId('count-view')).toHaveText('0');
  await expect(page.getByText('所选日期范围无数据', { exact: true })).toBeVisible();
});
