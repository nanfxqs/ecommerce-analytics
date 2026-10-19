import { test, expect } from '@playwright/test';
test('real bounded stream automatically publishes complete historical analysis', async ({ page }) => {
  test.skip(process.env.PUBLICATION_SMOKE !== '1', 'Requires completed isolated real YARN publication run on API8005');
  await page.goto('/');
  await expect(page.getByTestId('count-view')).toHaveText('59');
  await expect(page.getByTestId('count-addtocart')).toHaveText('2');
  await expect(page.getByTestId('count-visitors')).toHaveText('60');
  await page.getByRole('menuitem', { name: '实时监控' }).click();
  await expect(page.getByText('完整历史分析已通过校验并发布')).toBeVisible();
  await expect(page.getByText('已发布，可在历史分析导航查看完整结果。')).toBeVisible();
  await expect(page.getByText('已发布批次：', { exact: false })).toBeVisible();
  await page.screenshot({ path: '../.scratch/publication-monitor.png', fullPage: true });
  await page.reload();
  await expect(page.getByTestId('count-view')).toHaveText('59');
});
