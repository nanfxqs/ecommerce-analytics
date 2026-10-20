import { test, expect } from '@playwright/test';

test('historical charts preserve denominators and whole-range profiles across ID lookup', async ({ page }) => {
  const errors: string[] = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto('/');
  await page.getByLabel('开始日期').fill('1970-01-01');
  await page.getByLabel('结束日期').fill('1970-01-02');
  await page.getByRole('button', { name: '应用日期' }).click();
  const trend = page.getByRole('img', { name: 'UTC 每日行为与独立访客趋势' });
  for (const label of ['浏览（次）', '加购（次）', '购买（次）', '每日独立访客（人）']) {
    await expect(trend.getByText(label, { exact: true })).toBeVisible();
  }
  await page.getByRole('menuitem', { name: '购买关联' }).click();
  await expect(page.getByRole('img', { name: '浏览与加购购买关联率条形图' }).getByText('100.00%', { exact: true })).toBeVisible();
  await page.screenshot({ path: '.scratch/visual-checks/purchase.png', fullPage: true });
  await page.getByRole('menuitem', { name: '分类偏好' }).click();
  await expect(page.getByRole('img', { name: '分类关联覆盖率环形图' })).toBeVisible();
  await expect(page.getByRole('img', { name: '分类浏览次数 Top10 条形图' })).toBeVisible();
  await page.getByRole('combobox', { name: '分类图表指标' }).selectOption('transactions');
  await expect(page.getByRole('img', { name: '分类购买次数 Top10 条形图' }).getByText('2', { exact: true }).last()).toBeVisible();
  await page.screenshot({ path: '.scratch/visual-checks/categories.png', fullPage: true });
  await page.getByRole('menuitem', { name: '用户画像' }).click();
  await expect(page.getByTestId('population-total')).toHaveText('3');
  await expect(page.getByRole('img', { name: '全范围访客活跃天数分布' }).getByText('2（66.67%）', { exact: true })).toBeVisible();
  await expect(page.getByRole('img', { name: '全范围访客最近活跃间隔分布' })).toBeVisible();
  await page.getByLabel('访客 ID（精确查询）').fill('1');
  await page.getByRole('button', { name: '查询访客' }).click();
  await expect(page.getByTestId('profile-total')).toHaveText('1');
  await expect(page.getByTestId('population-total')).toHaveText('3');
  await expect(page.getByRole('img', { name: '访客 1 浏览分类偏好' })).toBeVisible();
  await page.screenshot({ path: '.scratch/visual-checks/profiles.png', fullPage: true });
  await page.getByLabel('访客 ID（精确查询）').fill('missing');
  await page.getByRole('button', { name: '查询访客' }).click();
  await expect(page.getByText('当前查询或页码无访客数据')).toBeVisible();
  await expect(page.getByTestId('population-total')).toHaveText('3');
  expect(errors).toEqual([]);
});

test('million-scale views and small purchases have independent readable historical and live panels', async ({ page }) => {
  const trend = [
    { day: '2015-01-01', view: 1000000, addtocart: 1000, transaction: 10, visitors: 500000 },
    { day: '2015-01-02', view: 2000000, addtocart: 1500, transaction: 20, visitors: 600000 },
  ];
  await page.route('**/api/history/overview*', route => route.fulfill({ json: {
    snapshot: { batch_id: 'scale-case', revision: 'fixed', timezone: 'UTC', complete: true,
      event_bounds: { start: '2015-01-01', end: '2015-01-02' }, updated_at: '2026-10-20T00:00:00Z' },
    selected_range: { start: '2015-01-01', end: '2015-01-02' },
    data: { counts: { view: 3000000, addtocart: 2500, transaction: 30 }, visitors: { visitors: 700000 }, trend },
  } }));
  await page.route('**/api/replay/datasets', route => route.fulfill({ json: [] }));
  await page.route('**/api/replay/current', route => route.fulfill({ json: {
    run_id: 'scale-case', phase: 'stopped', complete: false, confirmed: 3002530,
    snapshot: { run_id: 'scale-case', updated_at: '2026-10-20T00:00:00Z', timezone: 'UTC',
      processed_offsets: {}, visitors: 700000, overview: { view: 3000000, addtocart: 2500, transaction: 30 },
      quality: { valid_count: 3002530 }, invalid_reasons: {},
      daily: trend.flatMap(row => (['view', 'addtocart', 'transaction'] as const).map(event => ({ event_date: row.day, event, count: row[event] }))),
    },
  } }));
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto('/');
  const historical = page.getByRole('img', { name: 'UTC 每日行为与独立访客趋势' });
  await expect(historical.getByText('购买（次）', { exact: true })).toBeVisible();
  await expect(historical.getByText('20', { exact: true })).toBeVisible();
  const box = (await historical.boundingBox())!;
  await page.mouse.move(box.x + box.width / 3, box.y + 90);
  await expect(historical.getByText('购买：10 次', { exact: true })).toBeVisible();
  await expect(historical.getByText('浏览：1,000,000 次', { exact: true })).toBeVisible();
  await page.mouse.move(0, 0);
  await page.screenshot({ path: '.scratch/visual-checks/extreme-history.png', fullPage: true });
  await page.getByRole('menuitem', { name: '实时监控' }).click();
  const live = page.getByRole('img', { name: '本轮原始事件 UTC 分日趋势' });
  await expect(live.getByText('购买（次）', { exact: true })).toBeVisible();
  await expect(live.getByText('20', { exact: true })).toBeVisible();
  await expect(page.getByTestId('live-purchase')).toHaveText('30');
  await page.screenshot({ path: '.scratch/visual-checks/extreme-live.png', fullPage: true });
});

test('profile intervals use readable units and desktop navigation stays reachable while scrolling', async ({ page }) => {
  await page.route('**/api/history/profiles*', route => route.fulfill({ json: {
    snapshot: { batch_id: 'units', revision: 'fixed', timezone: 'UTC', complete: true,
      event_bounds: { start: '2015-01-01', end: '2015-01-03' }, updated_at: '2026-10-20T00:00:00Z' },
    selected_range: { start: '2015-01-01', end: '2015-01-03' },
    data: { total: 8, page: 1, page_size: 20, reference_time: '2015-01-03T00:00:00Z',
      visitors: [129600, 5400, 90, 45, 0, null, 86400, 3600].map((seconds, index) => ({
        visitorid: String(index), active_days: 1, events: 1, purchases: 0,
        last_event_time: '2015-01-01T00:00:00Z', recency_seconds: seconds, categories: [],
      })),
    },
  } }));
  await page.setViewportSize({ width: 1440, height: 650 });
  await page.goto('/');
  await page.getByRole('menuitem', { name: '用户画像' }).click();
  const table = page.locator('.ant-table');
  await expect(table.getByRole('columnheader', { name: '最近活跃间隔', exact: true })).toBeVisible();
  for (const text of ['1.5 天', '1.5 小时', '1.5 分钟', '45 秒', '0 秒', '—', '1 天', '1 小时']) {
    await expect(table.getByRole('cell', { name: text, exact: true })).toHaveCount(1);
  }
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await expect.poll(() => page.evaluate(() => window.scrollY)).toBeGreaterThan(100);
  const sidebar = page.locator('.sidebar');
  expect((await sidebar.boundingBox())!.y).toBeCloseTo(0);
  await page.getByRole('menuitem', { name: '行为概览' }).click();
  await expect(page.getByRole('heading', { name: '行为概览', exact: true })).toHaveCount(1);
  await page.setViewportSize({ width: 1440, height: 280 });
  await expect.poll(() => sidebar.evaluate(node => ({ height: node.clientHeight, scroll: node.scrollHeight, css: getComputedStyle(node).height }))).toMatchObject({ height: 280 });
  expect(await sidebar.evaluate(node => node.scrollHeight > node.clientHeight)).toBeTruthy();
  await sidebar.evaluate(node => { node.scrollTop = node.scrollHeight; });
  await expect(page.getByRole('menuitem', { name: '数据质量' })).toBeVisible();
  await page.setViewportSize({ width: 600, height: 650 });
  expect(await sidebar.evaluate(node => getComputedStyle(node).position)).toBe('relative');
});
