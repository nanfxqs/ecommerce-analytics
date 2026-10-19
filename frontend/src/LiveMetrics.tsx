import { useEffect, useRef } from 'react';
import { Card, Empty, Space, Statistic, Table, Typography } from 'antd';
import * as echarts from 'echarts';
export type LiveSnapshot = {
  run_id: string; updated_at: string; timezone: string; event_time_min: string | null; event_time_max: string | null;
  visitors: number; overview: Record<string, number>; quality: Record<string, number>;
  daily: { event_date: string; event: string; count: number }[]; invalid_reasons: Record<string, number>;
};
export function LiveMetrics({ snapshot }: { snapshot?: LiveSnapshot | null }) {
  const element = useRef<HTMLDivElement>(null), chart = useRef<echarts.ECharts | null>(null);
  useEffect(() => {
    if (!element.current || !snapshot) return;
    chart.current = echarts.init(element.current, undefined, { renderer: 'svg' });
    const resize = () => chart.current?.resize(); window.addEventListener('resize', resize);
    return () => { window.removeEventListener('resize', resize); chart.current?.dispose(); chart.current = null; };
  }, [!!snapshot]);
  useEffect(() => {
    if (!snapshot || !chart.current) return;
    const days = [...new Set(snapshot.daily.map(r => r.event_date))].sort();
    chart.current.setOption({ tooltip: { trigger: 'axis' }, legend: { data: ['浏览', '加购', '购买'] }, grid: { left: 60, right: 20, bottom: 45 }, xAxis: { type: 'category', data: days }, yAxis: { type: 'value', minInterval: 1 }, series: [['view', '浏览'], ['addtocart', '加购'], ['transaction', '购买']].map(([event, name]) => ({ name, type: 'line', showSymbol: days.length < 20, data: days.map(day => snapshot.daily.find(r => r.event_date === day && r.event === event)?.count || 0) })) });
  }, [snapshot]);
  if (!snapshot) return <Card className="section" title="本轮持续分析"><Empty description="等待首个已提交业务结果" /></Card>;
  const quality = snapshot.quality;
  return <>
    <Card className="section" title="本轮持续分析 · 已提交结果"><Typography.Paragraph type="secondary">统计本轮已处理的全部有效数据，不提供日期裁剪。业务结果更新时间 {snapshot.updated_at} · 原始事件 UTC 范围 {snapshot.event_time_min ?? '暂无有效时间'} 至 {snapshot.event_time_max ?? '暂无有效时间'}。访客为 visitorid 标识的行为主体。</Typography.Paragraph><Space size={45} wrap>{[
      ['live-valid', '有效行为事件', quality.valid_count], ['live-visitors', '本轮独立访客', snapshot.visitors],
      ['live-view', '浏览', snapshot.overview.view || 0], ['live-cart', '加购', snapshot.overview.addtocart || 0], ['live-purchase', '购买', snapshot.overview.transaction || 0],
    ].map(([id, title, value]) => <Statistic key={id} title={title} value={Number(value)} formatter={v => <span data-testid={id}>{Number(v).toLocaleString('en-US')}</span>} />)}</Space></Card>
    <Card className="section" title="行为趋势 · 原始事件 UTC 分日"><div ref={element} role="img" aria-label="本轮原始事件 UTC 分日趋势" style={{ height: 300 }} /><Table size="small" rowKey={r => r.event_date + ':' + r.event} dataSource={snapshot.daily} pagination={{ pageSize: 15 }} columns={[{ title: 'UTC 日期', dataIndex: 'event_date' }, { title: '行为', dataIndex: 'event', render: (value: string) => ({ view: '浏览', addtocart: '加购', transaction: '购买' }[value] || value) }, { title: '事件数', dataIndex: 'count' }]} /></Card>
    <Card className="section" title="持续质量统计"><Space size={40} wrap>{[
      ['consumed_count', '消费消息'], ['unique_source_count', '唯一源记录'], ['invalid_count', '异常记录'], ['retry_count', '源记录重试'], ['suspected_duplicate_count', '疑似内容重复'],
    ].map(([key, title]) => <Statistic key={key} title={title} value={quality[key] || 0} formatter={v => <span data-testid={'live-' + key}>{Number(v).toLocaleString('en-US')}</span>} />)}</Space><Typography.Paragraph type="secondary">重试消息按源身份精确去重；不同源行的相同内容保留。合法乱序不丢弃。异常与有效记录分别统计，异常不增加有效访客数。</Typography.Paragraph>{Object.keys(snapshot.invalid_reasons).length ? <Table rowKey="reason" pagination={false} dataSource={Object.entries(snapshot.invalid_reasons).map(([reason, count]) => ({ reason, count }))} columns={[{ title: '异常原因', dataIndex: 'reason' }, { title: '记录数', dataIndex: 'count' }]} /> : <Typography.Text type="secondary">当前没有异常记录</Typography.Text>}</Card>
  </>;
}
