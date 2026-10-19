import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Alert, Button, Card, Col, ConfigProvider, Empty, Layout, Menu, Row, Space, Spin, Statistic, Tag, Typography } from 'antd';
import zhCN from 'antd/locale/zh_CN';
import * as echarts from 'echarts';
import { historyOverview, type HistoryResponse, type Overview } from './api';
import './style.css';
import { ReplayPage } from './ReplayPage';

const { Title, Text } = Typography;
function Trend({ rows }: { rows: Overview['trend'] }) {
  const element = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const chart = echarts.init(element.current!);
    chart.setOption({ tooltip: { trigger: 'axis' }, legend: { bottom: 0 }, grid: { left: 60, right: 60, top: 25, bottom: 65 }, xAxis: { type: 'category', data: rows.map(r => r.day) }, yAxis: [{ type: 'value', name: '行为事件' }, { type: 'value', name: '独立访客' }], series: [
      ...(['view', 'addtocart', 'transaction'] as const).map((key, i) => ({ name: ['浏览', '加购', '购买'][i], type: 'line', data: rows.map(r => r[key]), smooth: false })),
      { name: '每日独立访客', type: 'line', yAxisIndex: 1, data: rows.map(r => r.visitors), lineStyle: { type: 'dashed' } }
    ] });
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(element.current!);
    return () => { observer.disconnect(); chart.dispose(); };
  }, [rows]);
  return <div role="img" aria-label="UTC 每日行为与独立访客趋势" ref={element} style={{ height: 340 }} />;
}

function App() {
  const [replay, setReplay] = useState(false);
  const [result, setResult] = useState<HistoryResponse<Overview> | null>(null);
  const [range, setRange] = useState<[string, string] | null>(null);
  const [draft, setDraft] = useState<[string, string]>(['', '']);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    let live = true;
    let controller: AbortController | undefined;
    let timer: ReturnType<typeof setTimeout>;
    const refresh = async () => {
      controller = new AbortController();
      try {
        const body = await historyOverview(range, controller.signal);
        if (!live) return;
        setResult(body); setError('');
        if (!range && body.selected_range) setDraft([body.selected_range.start, body.selected_range.end]);
      } catch (cause) {
        if (live && !(cause instanceof DOMException && cause.name === 'AbortError')) setError((cause as Error).message);
      } finally {
        if (live) { setLoading(false); timer = setTimeout(refresh, 2000); }
      }
    };
    setLoading(true); void refresh();
    return () => { live = false; controller?.abort(); clearTimeout(timer); };
  }, [range]);
  const snapshot = result?.snapshot;
  const data = result?.data;
  const apply = () => {
    if (!draft[0] || !draft[1] || draft[0] > draft[1]) { setError('请选择完整且顺序正确的 UTC 日期范围'); return; }
    setRange([...draft]);
  };
  return <ConfigProvider locale={zhCN} theme={{ token: { colorPrimary: '#2864b8', borderRadius: 10, fontFamily: 'Inter, system-ui, sans-serif' } }}>
    <Layout className="shell">
      <Layout.Sider theme="light" width={220} className="sidebar">
        <div className="brand">Retailrocket<Text type="secondary">电商行为分析</Text></div>
        <Menu onClick={({ key }) => setReplay(key === 'replay')} selectedKeys={[replay ? 'replay' : 'overview']} items={[{ key: 'replay', label: '实时监控' },{ key: 'history', type: 'group', label: '历史分析', children: [{ key: 'overview', label: '行为概览' }] }]} />
        <div className="sidebar-note">公开历史数据<br />UTC 事件时间</div>
      </Layout.Sider>
      <Layout.Content className="content">
        {replay ? <ReplayPage /> : <>
        <header><div><Text type="secondary">分析工作台 / 历史分析</Text><Title level={2}>行为概览</Title></div><Tag color="blue">历史数据回放</Tag></header>
        <Text type="secondary">浏览、加购与购买行为的历史统计，访客以 visitorid 区分。</Text>
        {error && <Alert type="error" showIcon message={error} className="section" />}
        {loading && !result && <Spin aria-label="正在查询" className="section" />}
        {!loading && result && !snapshot && <Card className="section"><Empty description="尚无成功发布的分析批次" /></Card>}
        {snapshot && <>
          <Card className="section" size="small"><Space wrap><Tag color="green">完整 · 已发布</Tag><Text>批次 {snapshot.batch_id}</Text><Text type="secondary">更新于 {new Date(snapshot.updated_at).toISOString()} · UTC</Text></Space><div className="metadata">事件范围 {snapshot.event_bounds.start ?? '无有效事件'} — {snapshot.event_bounds.end ?? '无有效事件'} · UTC</div></Card>
          {snapshot.event_bounds.start && <Card className="section" size="small"><Space wrap align="end"><label>开始日期<input aria-label="开始日期" type="date" value={draft[0]} onChange={e => setDraft([e.target.value, draft[1]])} /></label><label>结束日期<input aria-label="结束日期" type="date" value={draft[1]} onChange={e => setDraft([draft[0], e.target.value])} /></label><Button type="primary" onClick={apply} loading={loading}>应用日期</Button><Text type="secondary">含开始及结束日全天 · UTC</Text></Space></Card>}
          {data ? <>
            <Row gutter={[16, 16]} className="section">{([['view', '浏览事件', data.counts.view], ['addtocart', '加购事件', data.counts.addtocart], ['transaction', '购买事件', data.counts.transaction], ['visitors', '独立访客', data.visitors.visitors]] as const).map(([key, label, value]) => <Col span={6} key={key}><Card><Statistic title={label} value={value} formatter={() => <span data-testid={'count-' + key}>{value.toLocaleString('en-US')}</span>} /></Card></Col>)}</Row>
            <Card className="section" title="UTC 每日趋势"><Text type="secondary">查询范围 {result?.selected_range?.start} — {result?.selected_range?.end} · 独立访客跨日去重，每日人数不可相加。</Text>{data.trend.length ? <Trend rows={data.trend} /> : <Empty description="所选日期范围无数据" />}</Card>
          </> : <Card className="section"><Empty description="无有效事件数据" /></Card>}
        </>}
        <footer>Retailrocket 公开历史数据 · 页面每 2 秒查询已发布结果 · 历史活动不代表当前访问</footer>
        </>}
      </Layout.Content>
    </Layout>
  </ConfigProvider>;
}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
