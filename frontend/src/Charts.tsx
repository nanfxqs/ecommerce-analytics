import { useEffect, useRef } from 'react';
import { Empty, Typography } from 'antd';
import * as echarts from 'echarts';
import type { EChartsOption } from 'echarts';

export const behaviorColors = { view: '#2864b8', addtocart: '#d48806', transaction: '#c6425c', visitors: '#16866b' };
const number = (value: number) => value.toLocaleString('en-US', { maximumFractionDigits: 2 });
const axisNumber = (value: number) => value >= 100000000 ? number(value / 100000000) + '亿'
  : value >= 10000 ? number(value / 10000) + '万' : number(value);

function Chart({ option, label, height }: { option: EChartsOption; label: string; height: number }) {
  const element = useRef<HTMLDivElement>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  useEffect(() => {
    const instance = echarts.init(element.current!, undefined, { renderer: 'svg' });
    chart.current = instance;
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(element.current!);
    return () => { observer.disconnect(); instance.dispose(); chart.current = null; };
  }, []);
  useEffect(() => { chart.current?.setOption(option, { notMerge: true }); }, [option]);
  return <div className="chart" role="img" aria-label={label} ref={element} style={{ height }} />;
}

export type TrendRow = { day: string; view: number; addtocart: number; transaction: number; visitors?: number };
export function BehaviorTrend({ rows, visitors = false, label }: { rows: TrendRow[]; visitors?: boolean; label: string }) {
  if (!rows.length) return <Empty description="暂无有效行为趋势" />;
  const metrics: { key: keyof typeof behaviorColors; name: string; unit: string }[] = [
    { key: 'view', name: '浏览', unit: '次' }, { key: 'addtocart', name: '加购', unit: '次' },
    { key: 'transaction', name: '购买', unit: '次' },
    ...(visitors ? [{ key: 'visitors' as const, name: '每日独立访客', unit: '人' }] : []),
  ];
  const days = rows.map(row => row.day);
  const option: EChartsOption = {
    animation: false,
    tooltip: { trigger: 'axis', renderMode: 'richText', confine: true,
      formatter: (raw: unknown) => {
        const entry = (Array.isArray(raw) ? raw[0] : raw) as { dataIndex: number } | undefined;
        const row = entry ? rows[entry.dataIndex] : undefined;
        return row ? [row.day + ' · UTC', ...metrics.map(m => `${m.name}：${number(row[m.key] ?? 0)} ${m.unit}`)].join('\n') : '';
      } },
    axisPointer: { link: [{ xAxisIndex: 'all' }] },
    grid: metrics.map((_, i) => ({ left: 86, right: 32, top: 38 + i * 136, height: 92 })),
    xAxis: metrics.map((_, i) => ({ type: 'category', gridIndex: i, data: days, boundaryGap: false,
      axisLabel: { show: i === metrics.length - 1, hideOverlap: true }, axisTick: { show: false } })),
    yAxis: metrics.map((metric, i) => ({ type: 'value', gridIndex: i, min: 0, minInterval: 1,
      name: `${metric.name}（${metric.unit}）`, nameTextStyle: { color: behaviorColors[metric.key], fontWeight: 600 },
      axisLabel: { formatter: (v: number) => axisNumber(v) }, splitLine: { lineStyle: { color: '#edf1f7' } } })),
    series: metrics.map((metric, i) => ({ name: metric.name, type: 'line', xAxisIndex: i, yAxisIndex: i,
      data: rows.map(row => row[metric.key] ?? 0), showSymbol: rows.length < 20, symbolSize: 6,
      itemStyle: { color: behaviorColors[metric.key] }, lineStyle: { width: 2 },
      areaStyle: { color: behaviorColors[metric.key], opacity: .045 } })),
  };
  return <><Typography.Paragraph type="secondary" className="chart-note">共享 UTC 日期，各图纵轴独立且从 0 开始；曲线高度不能跨图比较数量。悬停任一子图可查看当天全部指标。</Typography.Paragraph>
    <Chart option={option} label={label} height={metrics.length * 136 + 40} /></>;
}

export type BarEntry = { name: string; value: number; share?: number | null; detail?: string; color?: string };
export function HorizontalBars({ rows, label, color = '#2864b8', percent = false }: { rows: BarEntry[]; label: string; color?: string; percent?: boolean }) {
  if (!rows.length) return <Empty description="暂无可视化数据" />;
  const format = (value: number) => percent ? value.toFixed(2) + '%' : number(value);
  const option: EChartsOption = {
    animation: false,
    grid: { left: 126, right: 156, top: 18, bottom: 38 },
    xAxis: { type: 'value', min: 0, minInterval: percent ? undefined : 1,
      name: percent ? '关联率（%）' : '数量', nameLocation: 'middle', nameGap: 26,
      axisLabel: { hideOverlap: true, formatter: (v: number) => percent ? number(v) + '%' : axisNumber(v) },
      splitLine: { lineStyle: { color: '#edf1f7' } } },
    yAxis: { type: 'category', inverse: true, data: rows.map(row => row.name),
      axisLabel: { width: 112, overflow: 'truncate' }, axisLine: { show: false }, axisTick: { show: false } },
    tooltip: { trigger: 'axis', renderMode: 'richText', confine: true,
      formatter: (raw: unknown) => {
        const entry = (Array.isArray(raw) ? raw[0] : raw) as { dataIndex: number } | undefined;
        const row = entry ? rows[entry.dataIndex] : undefined;
        return row ? `${row.name}\n${format(row.value)}${row.share == null ? '' : `（${(row.share * 100).toFixed(2)}%）`}${row.detail ? '\n' + row.detail : ''}` : '';
      } },
    series: [{ type: 'bar', barMaxWidth: 24, itemStyle: { color, borderRadius: [0, 4, 4, 0] },
      data: rows.map(row => ({ value: row.value, itemStyle: row.color ? { color: row.color } : undefined })),
      label: { show: true, position: 'right', distance: 8, color: '#334155',
        formatter: (raw: unknown) => {
          const i = (raw as { dataIndex: number }).dataIndex;
          const row = rows[i];
          return `${format(row.value)}${row.share == null ? '' : `（${(row.share * 100).toFixed(2)}%）`}`;
        } } }],
  };
  return <div className="chart-scroll"><Chart option={option} label={label} height={Math.max(200, rows.length * 38 + 64)} /></div>;
}

export function CoverageRing({ associated, unknown }: { associated: number; unknown: number }) {
  if (associated + unknown === 0) return <Empty description="暂无分类覆盖数据" />;
  const option: EChartsOption = {
    animation: false,
    tooltip: { trigger: 'item', renderMode: 'richText', confine: true, formatter: '{b}\n{c} 条（{d}%）' },
    legend: { bottom: 2, left: 'center', selectedMode: false,
      formatter: (name: string) => `${name}：${number(name === '已关联分类' ? associated : unknown)}` },
    graphic: [{ type: 'text', left: 'center', top: '42%', style: { text: '分类覆盖', fill: '#334155', fontSize: 18, fontWeight: 600 } }],
    series: [{ type: 'pie', radius: ['48%', '70%'], center: ['50%', '45%'], label: { show: false },
      data: [{ name: '已关联分类', value: associated, itemStyle: { color: '#16866b' } },
        { name: 'unknown', value: unknown, itemStyle: { color: '#c9d2df' } }] }],
  };
  return <Chart option={option} label="分类关联覆盖率环形图" height={310} />;
}
