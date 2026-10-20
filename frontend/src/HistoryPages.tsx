import { useEffect, useState } from 'react';
import { Button, Card, Col, Empty, Row, Space, Statistic, Table, Typography } from 'antd';
import type { HistoryResponse } from './api';
import { CategoryCharts, ProfileCharts, PurchaseChart } from './HistoryVisuals';

export type HistoryPage = 'overview' | 'purchase' | 'categories' | 'profiles' | 'quality';
export const historyLabels: Record<HistoryPage, string> = { overview: '行为概览', purchase: '购买关联', categories: '分类偏好', profiles: '用户画像', quality: '数据质量' };
export type HistoryOptions = { visitorid: string; page_number: number; page_size: number; top_n: number; scope: 'batch' | 'range'; limit: number };
export const initialHistoryOptions: HistoryOptions = { visitorid: '', page_number: 1, page_size: 20, top_n: 5, scope: 'batch', limit: 100 };
export async function fetchHistoryPage(page: HistoryPage, range: [string, string] | null, options: HistoryOptions, signal: AbortSignal): Promise<HistoryResponse<unknown>> {
  const params = new URLSearchParams();
  if (range) { params.set('start', range[0]); params.set('end', range[1]); }
  if (page === 'profiles') {
    if (options.visitorid) params.set('visitorid', options.visitorid);
    for (const key of ['page_number', 'page_size', 'top_n'] as const) params.set(key, String(options[key]));
  }
  if (page === 'quality') { params.set('scope', options.scope); params.set('limit', String(options.limit)); }
  const response = await fetch(`/api/history/${page}?${params}`, { signal });
  if (!response.ok) { const body = await response.json(); throw new Error(typeof body.detail === 'string' ? body.detail : '查询失败，请检查 UTC 日期及分页参数'); }
  return response.json();
}

type Conversion = Record<string, number | null>;
export type CategoryRow = { categoryid?: string; itemid?: string; events: number; views: number; addtocarts: number; transactions: number };
type Categories = { items: CategoryRow[]; categories: CategoryRow[]; coverage: { total: number; associated: number; unknown: number; rate: number | null } };
type Visitor = { visitorid: string; active_days: number; events: number; purchases: number; last_event_time: string; recency_seconds: number; categories: { categoryid: string; views: number; purchases: number }[] };
type Bucket = { label: string; count: number; share: number | null };
export type Profiles = { total: number; page: number; page_size: number; reference_time: string | null; visitors: Visitor[]; population?: { total: number; scope: string; active_days: Bucket[]; recency: Bucket[] } };
type Quality = { batch_quality: Record<string, number>; range_quality: { valid_count: number; invalid_count: number } | null; reasons: { invalid_reason: string; count: number }[]; records: Record<string, unknown>[]; anomaly_scope: 'batch' | 'range' };
export function formatRecency(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return '—';
  const [scale, unit] = seconds >= 86400 ? [86400, '天'] : seconds >= 3600 ? [3600, '小时'] : seconds >= 60 ? [60, '分钟'] : [1, '秒'];
  return `${Number((seconds / scale).toFixed(2))} ${unit}`;
}
const { Text } = Typography;
const metrics = (rows: [string, string, number | null | undefined][]) => <Row gutter={[16, 16]}>{rows.map(([key, label, value]) => <Col {...(rows.length === 5 ? { flex: '1 1 170px' } : { span: 6 })} key={key}><Card><Statistic title={label} value={value ?? 0} formatter={() => <span data-testid={key}>{value == null ? '无数据' : value.toLocaleString('en-US')}</span>} /></Card></Col>)}</Row>;
const categoryColumns = (key: 'categoryid' | 'itemid') => [{ title: key === 'categoryid' ? '分类 ID' : '商品 ID', dataIndex: key }, { title: '行为事件', dataIndex: 'events' }, { title: '浏览', dataIndex: 'views' }, { title: '加购', dataIndex: 'addtocarts' }, { title: '购买', dataIndex: 'transactions' }];

export function HistoryPages({ page, payload, options, onOptions }: { page: Exclude<HistoryPage, 'overview'>; payload: unknown; options: HistoryOptions; onOptions: (next: HistoryOptions) => void }) {
  const [visitorDraft, setVisitorDraft] = useState(options.visitorid);
  const [pageDraft, setPageDraft] = useState(String(options.page_number));
  useEffect(() => setPageDraft(String(options.page_number)), [options.page_number]);
  if (page === 'purchase') {
    const data = payload as Conversion | null;
    if (!data) return <Empty description="无有效事件数据" />;
    return <div className="section">
      {metrics([['view-visitors', '浏览访客', data.view_visitors], ['cart-visitors', '加购访客', data.cart_visitors], ['purchase-visitors', '购买访客', data.purchase_visitors], ['view-purchase-visitors', '浏览与购买访客交集', data.view_purchase_visitors], ['cart-purchase-visitors', '加购与购买访客交集', data.cart_purchase_visitors]])}
      <Card className="section"><Space size="large"><div>浏览购买交集 / 浏览访客：<strong data-testid="view-purchase-rate">{data.view_purchase_rate == null ? '无数据' : (data.view_purchase_rate * 100).toFixed(2) + '%'}</strong></div><div>加购购买交集 / 加购访客：<strong data-testid="cart-purchase-rate">{data.cart_purchase_rate == null ? '无数据' : (data.cart_purchase_rate * 100).toFixed(2) + '%'}</strong></div></Space><p>同一范围内访客交集，不要求事件先后；这是购买关联，不是严格漏斗，也不是事件数量比。</p></Card>
      <PurchaseChart data={data} />
    </div>;
  }
  if (page === 'categories') {
    const data = payload as Categories | null;
    if (!data || !data.coverage.total) return <Card className="section"><Empty description="所选日期范围无商品分类数据" /></Card>;
    return <><Card className="section">分类关联覆盖率：<strong>{data.coverage.rate == null ? '无数据' : (data.coverage.rate * 100).toFixed(2) + '%'}</strong>；未知分类：{data.coverage.unknown} / {data.coverage.total} 条行为<p>分类展示匿名原始编号；unknown 表示行为发生时尚无可用分类。</p></Card><CategoryCharts categories={data.categories} coverage={data.coverage} /><Card className="section" title="热门商品（前 100 项）"><Table rowKey="itemid" dataSource={data.items} columns={categoryColumns('itemid')} pagination={{ pageSize: 20 }} /></Card><Card className="section" title="热门分类与分类购买情况"><Table rowKey="categoryid" dataSource={data.categories} columns={categoryColumns('categoryid')} pagination={{ pageSize: 20 }} /></Card></>;
  }
  if (page === 'profiles') {
    const data = payload as Profiles | null;
    return <><Card className="section"><Space wrap align="end"><label>访客 ID（精确查询）<input aria-label="访客 ID（精确查询）" value={visitorDraft} onChange={e => setVisitorDraft(e.target.value)} /></label><Button onClick={() => { setPageDraft('1'); onOptions({ ...options, visitorid: visitorDraft.trim(), page_number: 1 }); }}>查询访客</Button><label>每页访客数<select aria-label="每页访客数" value={options.page_size} onChange={e => { setPageDraft('1'); onOptions({ ...options, page_size: Number(e.target.value), page_number: 1 }); }}>{[20, 50, 100].map(n => <option key={n}>{n}</option>)}</select></label><label>浏览偏好分类数<input aria-label="浏览偏好分类数" type="number" min={1} max={50} value={options.top_n} onChange={e => { const n = Number(e.target.value); if (n >= 1 && n <= 50) onOptions({ ...options, top_n: n }); }} /></label><label>页码<input aria-label="页码" type="number" min={1} value={pageDraft} onChange={e => setPageDraft(e.target.value)} /></label><Button onClick={() => { const n = Number(pageDraft); if (Number.isInteger(n) && n >= 1) onOptions({ ...options, page_number: n }); }}>跳转页码</Button></Space><p>访客仅表示 visitorid 标识的行为主体，不代表注册会员或自然人数。</p><Text type="secondary">匹配访客 <span data-testid="profile-total">{data?.total ?? 0}</span> · 最近活跃间隔基准：{data?.reference_time ?? '无数据'} UTC</Text></Card>
      <ProfileCharts data={data} />
      {!data?.visitors.length ? <Card className="section"><Empty description="当前查询或页码无访客数据" /></Card> : <Card className="section"><Table rowKey="visitorid" dataSource={data.visitors} pagination={false} columns={[{ title: '访客 ID', dataIndex: 'visitorid' }, { title: '活跃天数', dataIndex: 'active_days' }, { title: '事件次数', dataIndex: 'events' }, { title: '购买次数', dataIndex: 'purchases' }, { title: '最近活跃（UTC）', dataIndex: 'last_event_time' }, { title: '最近活跃间隔', dataIndex: 'recency_seconds', render: formatRecency }]} expandable={{ expandedRowRender: row => <div>按浏览次数降序，同次数按分类 ID 排序；购买次数不计入偏好得分。{row.categories.length ? row.categories.map(c => <p key={c.categoryid}>分类 {c.categoryid} · 浏览 {c.views} · 购买 {c.purchases}</p>) : <p>无浏览分类偏好</p>}</div>, defaultExpandAllRows: true }} /><Space><Button disabled={options.page_number <= 1} onClick={() => { setPageDraft(String(options.page_number - 1)); onOptions({ ...options, page_number: options.page_number - 1 }); }}>上一页</Button><Text>第 {data.page} 页</Text><Button disabled={options.page_number * options.page_size >= data.total} onClick={() => { setPageDraft(String(options.page_number + 1)); onOptions({ ...options, page_number: options.page_number + 1 }); }}>下一页</Button></Space></Card>}
    </>;
  }
  const data = payload as Quality | null;
  if (!data) return <Empty description="暂无质量统计" />;
  return <><Card className="section" title="所选 UTC 日期范围的记录质量"><Space>有效行为：<strong>{data.range_quality?.valid_count ?? 0}</strong>可确定日期的异常：<strong>{data.range_quality?.invalid_count ?? 0}</strong></Space><p>时间无效的异常记录无法归属日期，仅包含在下方批次统计。</p></Card><Card className="section" title="整个批次的采集质量（不受日期筛选影响）">{metrics(([['input_count','输入记录'],['consumed_count','消费消息'],['unique_source_count','唯一源记录'],['valid_count','有效行为'],['invalid_count','异常记录'],['retry_count','重试消息'],['suspected_duplicate_count','疑似重复']] as const).map(([key,label]) => ['quality-'+key,label,data.batch_quality[key]]))}<p>消费 = 唯一源记录 + 重试消息；唯一源记录 = 有效 + 异常。疑似重复为重叠统计，不从有效行为扣减。</p></Card><Card className="section" title="异常原因与原始记录"><Space wrap><label>异常查看范围<select aria-label="异常查看范围" value={options.scope} onChange={e => onOptions({ ...options, scope: e.target.value as 'batch' | 'range' })}><option value="batch">整个批次（包含无法确定日期的记录）</option><option value="range">所选 UTC 日期（仅包含时间有效的记录）</option></select></label><label>显示异常记录上限<select aria-label="显示异常记录上限" value={options.limit} onChange={e => onOptions({ ...options, limit: Number(e.target.value) })}>{[100,250,500].map(n => <option key={n}>{n}</option>)}</select></label></Space><p>每条记录显示首个校验失败原因；按数据版本、源文件、源行号排序，原因统计覆盖整个选定范围。</p>{data.reasons.length ? <><Table rowKey="invalid_reason" dataSource={data.reasons} columns={[{ title: '异常原因', dataIndex: 'invalid_reason' }, { title: '记录数', dataIndex: 'count' }]} pagination={false} /><Table scroll={{ x: 1200 }} rowKey={r => `${r.dataset_version}:${r.source_file}:${r.source_line}`} dataSource={data.records} columns={['dataset_version','source_file','source_line','timestamp','event_time','visitorid','event','itemid','transactionid','invalid_reason'].map(key => ({ title: key, dataIndex: key, render: (v: unknown) => v == null ? '无数据' : String(v) }))} pagination={{ pageSize: 20 }} /></> : <Empty description="该范围没有异常记录" />}</Card></>;
}
