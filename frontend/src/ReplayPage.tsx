import React, { useEffect, useState } from 'react';
import { Alert, Button, Card, Empty, InputNumber, Select, Space, Statistic, Table, Tag, Typography } from 'antd';
import { LiveMetrics, type LiveSnapshot } from './LiveMetrics';
type Dataset = { id: string; label: string; rows: number };
type Run = { snapshot?: LiveSnapshot | null; run_id: string; phase: string; complete: boolean; confirmed: number; rate?: number; lag?: number; ends?: Record<string, number>; committed?: Record<string, number>; result_offsets?: Record<string, number>; samples?: unknown[]; updated_at?: string; error?: string; observation_error?: string };
async function request(path: string, init?: RequestInit) {
  const response = await fetch('/api/replay/' + path, init);
  if (!response.ok) { const body = await response.json(); throw new Error(body.detail || '回放服务暂不可用'); }
  return response.json();
}
export function ReplayPage() {
  const [datasets, setDatasets] = useState<Dataset[]>([]), [dataset, setDataset] = useState('small'), [rate, setRate] = useState(5);
  const [run, setRun] = useState<Run | null>(null), [error, setError] = useState(''), [busy, setBusy] = useState(false);
  useEffect(() => {
    let live = true; let timer: ReturnType<typeof setTimeout>;
    request('datasets').then(body => { if (live) { setDatasets(body); if (body.length) setDataset(body[0].id); } }).catch(e => { if (live) setError(e.message); });
    const poll = async () => { try { const body = await request('current'); if (live) setRun(body); } catch (e) { if (live) setError((e as Error).message); } finally { if (live) timer = setTimeout(poll, 2000); } };
    void poll(); return () => { live = false; clearTimeout(timer); };
  }, []);
  const start = async () => { setBusy(true); setError(''); try { setRun(await request('runs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ dataset, rate }) })); } catch (e) { setError((e as Error).message); } finally { setBusy(false); } };
  const active = run && !['completed', 'stopped'].includes(run.phase);
  const phases: Record<string, string> = { starting: '启动中', running: '回放中', draining: '正在消化积压', completed: '回放处理完成', failed: '失败 · 需要处理' };
  const partitions = Object.entries(run?.ends || {}).map(([partition, end]) => ({ partition, end, committed: run?.committed?.[partition] || 0, result: run?.result_offsets?.[partition] || 0, lag: end - (run?.committed?.[partition] || 0) }));
  return <>
    <header><Typography.Title level={2}>历史数据回放监控</Typography.Title><Tag color="blue">历史数据回放</Tag></header>
    <Typography.Paragraph type="secondary">当前处理时间用于速率和积压；原始事件时间为历史 UTC 时间。关闭浏览器后后台继续运行。</Typography.Paragraph>
    {error && <Alert type="error" message={error} showIcon className="section" />}
    <Card className="section" title="启动有界回放"><Space wrap><Select aria-label="数据版本" value={dataset} onChange={setDataset} disabled={!!active} style={{ width: 260 }} options={datasets.map(d => ({ value: d.id, label: `${d.label}（${d.rows.toLocaleString()} 条）` }))} /><label>每秒速率 <InputNumber aria-label="每秒速率" min={0.1} max={1000000} value={rate} onChange={v => setRate(v || 5)} disabled={!!active} /></label><Button type="primary" onClick={start} loading={busy} disabled={!!active || !datasets.length}>启动回放</Button></Space><Typography.Paragraph type="secondary">每轮只播放一次，不自动循环；同时只允许一轮运行。</Typography.Paragraph></Card>
    {run ? <>
      <Card className="section"><Space wrap><Tag color={run.complete ? 'green' : 'orange'}>{phases[run.phase] || run.phase}</Tag><Typography.Text>轮次 {run.run_id}</Typography.Text><Typography.Text>{run.complete ? '回放范围处理完整 · 尚非已发布历史批次' : '部分结果 · 回放范围尚未完整处理'}</Typography.Text><Typography.Text type="secondary">更新时间 {run.updated_at || '等待首次观测'} · 当前处理时间</Typography.Text></Space></Card>
      {(run.error || run.observation_error) && <Alert type="error" message={run.error || run.observation_error} className="section" />}
      <Card className="section"><Space size={60} wrap><Statistic title="生产确认消息量" value={run.confirmed} /><Statistic title="生产确认速率（条/秒）" value={run.rate || 0} precision={2} /><Statistic title="积压（Kafka 末端 − 已提交检查点）" value={run.lag ?? 0} /></Space></Card>
      <LiveMetrics snapshot={run.snapshot} />
      <Card className="section" title="分区进度（独占末端 offset）"><Typography.Paragraph type="secondary">结果覆盖 offset 与 Spark 已成功提交检查点分别展示。</Typography.Paragraph><Table pagination={false} rowKey="partition" dataSource={partitions} columns={[{ title: '分区', dataIndex: 'partition' }, { title: 'Kafka 末端', dataIndex: 'end' }, { title: '已提交检查点', dataIndex: 'committed' }, { title: '持久结果覆盖', dataIndex: 'result' }, { title: '积压', dataIndex: 'lag' }]} /></Card>
      <Card className="section" title="最近原始消息样本 · 清洗前 · 最多 20 条">{run.samples?.length ? <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere' }}>{JSON.stringify(run.samples.slice(0, 20), null, 2)}</pre> : <Empty description="等待原始消息样本" />}</Card>
    </> : <Empty description="尚未启动回放轮次" />}
  </>;
}
