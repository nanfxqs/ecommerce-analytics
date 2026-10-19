import { test, expect } from '@playwright/test';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
test.skip(!process.env.REPLAY_RECOVERY_LIVE,'Requires isolated actual worker/driver fault fixture');
test('manual recovery resumes same checkpoint and exactly deduplicates ACK uncertainty',async({page})=>{
  const before=JSON.parse(readFileSync('/tmp/ecommerce-v2-agent/recovery-fault-before.json','utf8'));
  await page.goto('/'); await page.getByText('实时监控',{exact:true}).click();
  await expect(page.getByText('等待手动恢复',{exact:true})).toBeVisible();
  expect((await (await page.request.get('/api/replay/current')).json()).run_id).toBe(before.run_id);
  await page.getByRole('button',{name:'恢复此轮次',exact:true}).click();
  await expect(page.getByText('回放处理完成',{exact:true})).toBeVisible({timeout:210000});
  const after=await(await page.request.get('/api/replay/current')).json();
  expect(after.run_id).toBe(before.run_id); expect(after.cursor).toBe(61); expect(after.lag).toBe(0);
  expect(after.snapshot.quality).toEqual({consumed_count:62,unique_source_count:61,valid_count:61,invalid_count:0,retry_count:1,suspected_duplicate_count:0});
  expect(after.snapshot.overview).toEqual({view:59,addtocart:2}); expect(after.snapshot.visitors).toBe(60);
  expect(after.stream_released).toBe(true);
  mkdirSync('../docs/evidence/v2-recovery',{recursive:true});writeFileSync('../docs/evidence/v2-recovery/recovered.json',JSON.stringify({before,after},null,2));
});
