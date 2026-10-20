import { useState } from 'react';
import { Card, Col, Empty, Row, Space, Typography } from 'antd';
import { CoverageRing, HorizontalBars, behaviorColors } from './Charts';
import type { CategoryRow, Profiles } from './HistoryPages';

export function PurchaseChart({ data }: { data: Record<string, number | null> }) {
  const rows = [
    { name: '浏览购买关联率', rate: data.view_purchase_rate, numerator: data.view_purchase_visitors, denominator: data.view_visitors },
    { name: '加购购买关联率', rate: data.cart_purchase_rate, numerator: data.cart_purchase_visitors, denominator: data.cart_visitors },
  ].flatMap(row => row.rate == null ? [] : [{ name: row.name, value: row.rate * 100,
    detail: `交集 ${row.numerator?.toLocaleString('en-US')} / ${row.denominator?.toLocaleString('en-US')} 名${row.name.startsWith('浏览') ? '浏览' : '加购'}访客` }]);
  return <Card className="section" title="购买关联率对比">
    <Typography.Paragraph type="secondary">两项使用各自访客分母，横轴为真实百分比且从 0 开始；文字标注小比例。不表示行为先后或严格转化漏斗。</Typography.Paragraph>
    {rows.length ? <HorizontalBars rows={rows} label="浏览与加购购买关联率条形图" percent /> : <Empty description="没有可用的访客分母" />}
  </Card>;
}

const categoryMetrics = {
  views: { name: '浏览', color: behaviorColors.view },
  addtocarts: { name: '加购', color: behaviorColors.addtocart },
  transactions: { name: '购买', color: behaviorColors.transaction },
};
export function CategoryCharts({ categories, coverage }: { categories: CategoryRow[]; coverage: { associated: number; unknown: number } }) {
  const [metric, setMetric] = useState<keyof typeof categoryMetrics>('views');
  const current = categoryMetrics[metric];
  const rows = [...categories].filter(row => row[metric] > 0)
    .sort((a, b) => b[metric] - a[metric] || String(a.categoryid).localeCompare(String(b.categoryid)))
    .slice(0, 10).map(row => ({ name: row.categoryid === 'unknown' ? 'unknown（未知）' : '分类 ' + row.categoryid,
      value: row[metric], color: row.categoryid === 'unknown' ? '#a9b5c7' : current.color }));
  return <Row gutter={[16, 16]} className="section">
    <Col xs={24} xl={8}><Card title="分类关联覆盖"><CoverageRing associated={coverage.associated} unknown={coverage.unknown} />
      <Typography.Text type="secondary">已关联与 unknown 互斥；unknown 表示事件当时无可用分类。</Typography.Text></Card></Col>
    <Col xs={24} xl={16}><Card title="热门分类 Top10" extra={<select aria-label="分类图表指标" value={metric}
      onChange={event => setMetric(event.target.value as keyof typeof categoryMetrics)} style={{ width: 100 }}>
      {Object.entries(categoryMetrics).map(([value, item]) => <option key={value} value={value}>{item.name}</option>)}
    </select>}>
      <Typography.Paragraph type="secondary">按{current.name}次数独立排名，每次只画一个指标，不与其他数量级混用；完整分类仍见下表。</Typography.Paragraph>
      <HorizontalBars rows={rows} color={current.color} label={`分类${current.name}次数 Top10 条形图`} />
    </Card></Col>
  </Row>;
}

export function ProfileCharts({ data }: { data: Profiles | null }) {
  const [selected, setSelected] = useState('');
  if (!data) return null;
  const population = data.population;
  const visitor = data.visitors.find(row => row.visitorid === selected) || data.visitors[0];
  return <>
    <Card className="section" title="全范围访客群体概览">
      <Typography.Paragraph type="secondary">所选 UTC 日期范围全部活跃访客，共 <strong data-testid="population-total">{population?.total.toLocaleString('en-US') ?? '等待统计'}</strong> 名；不随列表分页或访客 ID 查询变化。每位访客在每张分布图仅进入一个分组。</Typography.Paragraph>
      {!population ? <Empty description="群体统计尚未就绪，请重启演示后台加载新查询" /> : population.total ? <Row gutter={[24, 16]}>
        <Col xs={24} xl={12}><Typography.Title level={5}>活跃天数分布</Typography.Title>
          <HorizontalBars label="全范围访客活跃天数分布" rows={population.active_days.map(row => ({ name: row.label, value: row.count, share: row.share }))} /></Col>
        <Col xs={24} xl={12}><Typography.Title level={5}>最近活跃间隔分布</Typography.Title>
          <HorizontalBars label="全范围访客最近活跃间隔分布" color="#16866b" rows={population.recency.map(row => ({ name: row.label, value: row.count, share: row.share }))} /></Col>
      </Row> : <Empty description="所选日期范围暂无活跃访客" />}
      <Typography.Text type="secondary">柱旁为访客数和全范围占比。最近活跃以范围内最新事件 {data.reference_time ?? '无数据'} UTC 为基准，不是距今天多久，也不直接表示流失。</Typography.Text>
    </Card>
    {visitor && <Card className="section" title="单个访客 · 浏览分类偏好">
      <Space wrap><label>图表访客<select aria-label="图表访客" value={visitor.visitorid} style={{ minWidth: 180 }} onChange={event => setSelected(event.target.value)}>
        {data.visitors.map(row => <option key={row.visitorid} value={row.visitorid}>访客 {row.visitorid}</option>)}
      </select></label>
        <Typography.Text type="secondary">从当前页选择，或用上方访客 ID 精确查询。按当前查询的 TopN 展示，不代表完整分类占比。</Typography.Text></Space>
      {visitor.categories.length ? <HorizontalBars label={`访客 ${visitor.visitorid} 浏览分类偏好`} rows={visitor.categories.map(row => ({
        name: row.categoryid === 'unknown' ? 'unknown（未知）' : '分类 ' + row.categoryid,
        value: row.views, color: row.categoryid === 'unknown' ? '#a9b5c7' : undefined,
        detail: `浏览 ${row.views} 次；购买 ${row.purchases} 次（不混入浏览偏好分值）`,
      }))} /> : <Empty description="该访客没有浏览分类偏好" />}
    </Card>}
  </>;
}
