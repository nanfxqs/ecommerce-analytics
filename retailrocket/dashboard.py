"""Launch: streamlit run retailrocket/dashboard.py -- <publication store>."""
import sys
import pandas as pd
import plotly.express as px
import streamlit as st
from retailrocket.quality_view import render_anomalies
from retailrocket.publication import current_snapshot
from retailrocket.query import overview, quality_summary, date_bounds, visitor_conversion, daily_trend, range_quality

st.set_page_config(page_title='Retailrocket 历史行为分析', layout='wide')
st.title('Retailrocket 历史数据回放')
try:
    snapshot = current_snapshot(sys.argv[1])
except FileNotFoundError:
    st.info('尚无成功发布的分析批次')
    st.stop()
root = snapshot.directory
st.caption(f'批次 {snapshot.batch_id} · UTC · 历史事件时间，不是回放时间')
bounds = date_bounds(root)
if bounds[0] is None:
    st.info('无有效事件数据')
    st.subheader('整个批次的采集质量')
    st.json(quality_summary(root))
    render_anomalies(root)
    st.stop()
st.caption(f'数据范围 {bounds[0]} — {bounds[1]} UTC')
selected = st.date_input('日期范围（UTC）', value=bounds, min_value=bounds[0], max_value=bounds[1])
if len(selected) != 2:
    st.info('请选择开始和结束日期')
    st.stop()
start, end = selected
st.caption(f'包含 {start} 00:00:00 UTC 至 {end} 23:59:59.999 UTC（结束日全天）')
page = st.sidebar.radio('分析页面', ['概览', '购买关联', '分类偏好', '用户画像', '数据质量'])
if page == '概览':
    counts = overview(root / 'events', start, end)
    visitors = visitor_conversion(root, start, end)
    for column, (key, label) in zip(st.columns(3), [('view', '浏览事件'), ('addtocart', '加购事件'), ('transaction', '购买事件')]):
        column.metric(label, counts[key])
    st.metric('独立访客', visitors['visitors'])
    trend = daily_trend(root, start, end)
    if not trend:
        st.info('所选日期范围无数据')
    else:
        frame = pd.DataFrame(trend)
        st.plotly_chart(px.line(frame, x='day', y=['view', 'addtocart', 'transaction'], labels={'day':'UTC 日期','value':'事件数','variable':'行为'}), use_container_width=True)
        st.plotly_chart(px.line(frame, x='day', y='visitors', labels={'day':'UTC 日期','visitors':'每日独立访客'}), use_container_width=True)
        st.caption('范围独立访客跨日去重；每日独立访客不可直接相加。')
elif page == '购买关联':
    metrics = visitor_conversion(root, start, end)
    for key, label in [('view_visitors','浏览访客'), ('cart_visitors','加购访客'), ('purchase_visitors','购买访客'), ('view_purchase_visitors','浏览与购买访客交集'), ('cart_purchase_visitors','加购与购买访客交集')]:
        st.metric(label, metrics[key])
    for key, label in [('view_purchase_rate','浏览购买交集 / 浏览访客'), ('cart_purchase_rate','加购购买交集 / 加购访客')]:
        st.metric(label, '无数据' if metrics[key] is None else f'{metrics[key]:.2%}')
    st.caption('同一范围内访客交集，不要求事件先后；这是购买关联，不是严格漏斗，也不是事件数量比。')
elif page == '分类偏好':
    from retailrocket.category_view import render
    render(root, start, end)
elif page == '用户画像':
    from retailrocket.profile_view import render
    render(root, start, end)
else:
    st.subheader('所选 UTC 日期范围的记录质量')
    ranged = range_quality(root, start, end)
    st.metric('有效行为', ranged['valid_count'])
    st.metric('可确定事件日期的异常记录', ranged['invalid_count'])
    st.caption('时间无效的异常记录无法归属日期，仅包含在下方批次统计。')
    st.subheader('整个批次的采集质量（不受日期筛选影响）')
    quality = quality_summary(root)
    labels = [('input_count', '输入记录'), ('consumed_count', '消费消息'), ('unique_source_count', '唯一源记录'), ('valid_count', '有效行为'), ('invalid_count', '异常记录'), ('suspected_duplicate_count', '疑似重复')]
    for column, (key, label) in zip(st.columns(6), labels):
        column.metric(label, quality[key])
    st.caption(f"消费 {quality['consumed_count']} = 唯一源记录 {quality['unique_source_count']} + 重试消息 {quality['retry_count']}；唯一源记录 = 有效 {quality['valid_count']} + 异常 {quality['invalid_count']}。疑似重复为重叠统计，不从有效行为扣减。")

    render_anomalies(root, start, end)
