import { Card, Empty, Space, Statistic, Table, Typography } from 'antd';
import { BehaviorTrend } from './Charts';
export type LiveSnapshot = {
  run_id: string; updated_at: string; result_received_at?: string; timezone: string; event_time_min: string | null; event_time_max: string | null;
  processed_offsets: Record<string, Record<string, number>>;
  visitors: number; overview: Record<string, number>; quality: Record<string, number>;
  daily: { event_date: string; event: string; count: number }[]; invalid_reasons: Record<string, number>;
};
export function LiveMetrics({ snapshot }: { snapshot?: LiveSnapshot | null }) {
  if (!snapshot) return <Card className="section" title="本轮持续分析"><Empty description="等待首个已提交业务结果" /></Card>;
  const quality = snapshot.quality;
  const daily = new Map(snapshot.daily.map(row => [row.event_date + ':' + row.event, row.count]));
  const trend = [...new Set(snapshot.daily.map(row => row.event_date))].sort().map(day => ({
    day, view: daily.get(day + ':view') || 0, addtocart: daily.get(day + ':addtocart') || 0,
    transaction: daily.get(day + ':transaction') || 0,
  }));
  return <section data-testid="live-result" data-run-id={snapshot.run_id} data-result-offsets={JSON.stringify(snapshot.processed_offsets)} data-result-received-at={snapshot.result_received_at || ""}>
    <Card className="section" title="本轮持续分析 · 已提交结果"><Typography.Paragraph type="secondary">统计本轮已处理的全部有效数据，不提供日期裁剪。结果接收时间（本机 UTC）<span data-testid="result-received-at">{snapshot.result_received_at || '等待接收时间'}</span> · 引擎记录时间（节点 UTC）{snapshot.updated_at} · 原始事件 UTC 范围 {snapshot.event_time_min ?? '暂无有效时间'} 至 {snapshot.event_time_max ?? '暂无有效时间'}。访客为 visitorid 标识的行为主体。</Typography.Paragraph><Space size={45} wrap>{[
      ['live-valid', '有效行为事件', quality.valid_count], ['live-visitors', '本轮独立访客', snapshot.visitors],
      ['live-view', '浏览', snapshot.overview.view || 0], ['live-cart', '加购', snapshot.overview.addtocart || 0], ['live-purchase', '购买', snapshot.overview.transaction || 0],
    ].map(([id, title, value]) => <Statistic key={id} title={title} value={Number(value)} formatter={v => <span data-testid={id}>{Number(v).toLocaleString('en-US')}</span>} />)}</Space></Card>
    <Card className="section" title="行为趋势 · 原始事件 UTC 分日"><BehaviorTrend rows={trend} label="本轮原始事件 UTC 分日趋势" /><Table size="small" rowKey={r => r.event_date + ':' + r.event} dataSource={snapshot.daily} pagination={{ pageSize: 15 }} columns={[{ title: 'UTC 日期', dataIndex: 'event_date' }, { title: '行为', dataIndex: 'event', render: (value: string) => ({ view: '浏览', addtocart: '加购', transaction: '购买' }[value] || value) }, { title: '事件数', dataIndex: 'count' }]} /></Card>
    <Card className="section" title="持续质量统计"><Space size={40} wrap>{[
      ['consumed_count', '消费消息'], ['unique_source_count', '唯一源记录'], ['invalid_count', '异常记录'], ['retry_count', '源记录重试'], ['suspected_duplicate_count', '疑似内容重复'],
    ].map(([key, title]) => <Statistic key={key} title={title} value={quality[key] || 0} formatter={v => <span data-testid={'live-' + key}>{Number(v).toLocaleString('en-US')}</span>} />)}</Space><Typography.Paragraph type="secondary">重试消息按源身份精确去重；不同源行的相同内容保留。合法乱序不丢弃。异常与有效记录分别统计，异常不增加有效访客数。</Typography.Paragraph>{Object.keys(snapshot.invalid_reasons).length ? <Table rowKey="reason" pagination={false} dataSource={Object.entries(snapshot.invalid_reasons).map(([reason, count]) => ({ reason, count }))} columns={[{ title: '异常原因', dataIndex: 'reason', render: (reason: string) => ({ invalid_timestamp: '无效时间戳', missing_visitor: '缺少访客标识', missing_item: '缺少商品标识', invalid_event: '无效行为类型' }[reason] || reason) }, { title: '记录数', dataIndex: 'count' }]} /> : <Typography.Text type="secondary">当前没有异常记录</Typography.Text>}</Card>
  </section>;
}
