"""Launch: streamlit run retailrocket/dashboard.py -- <exported batch>."""
import json
import sys
from pathlib import Path
import duckdb
import plotly.express as px
import streamlit as st
from retailrocket.query import overview, quality_summary

st.set_page_config(page_title='Retailrocket 历史行为分析', layout='wide')
root = Path(sys.argv[1])
metadata = json.loads((root / 'metadata.json').read_text())
st.title('Retailrocket 历史数据回放')
st.caption(f"批次 {metadata['batch_id']} · UTC · 历史事件时间，不是回放时间")
quality = quality_summary(root)
st.subheader('批次质量摘要')
labels = [('input_count', '输入记录'), ('consumed_count', '消费消息'), ('unique_source_count', '唯一源记录'), ('valid_count', '有效行为'), ('invalid_count', '异常记录'), ('suspected_duplicate_count', '疑似重复')]
for column, (key, label) in zip(st.columns(6), labels):
    column.metric(label, quality[key])
st.caption(f"消费 {quality['consumed_count']} = 唯一源记录 {quality['unique_source_count']} + 重试消息 {quality['retry_count']}；唯一源记录 = 有效 {quality['valid_count']} + 异常 {quality['invalid_count']}。输入为采集前文件范围；疑似重复是内容相同但源行不同的重叠统计，不从有效行为扣减。")
with duckdb.connect() as connection:
    bounds = connection.execute('SELECT min(cast(event_time AS DATE)), max(cast(event_time AS DATE)) FROM read_parquet(?)', [str(root / 'events' / '*.parquet')]).fetchone()
if bounds[0] is None:
    st.info('无数据')
    st.stop()
st.caption(f'数据范围 {bounds[0]} — {bounds[1]} UTC')
selected = st.date_input('日期范围（UTC）', value=bounds, min_value=bounds[0], max_value=bounds[1])
if len(selected) != 2:
    st.stop()
counts = overview(root / 'events', *selected)
for column, (event, label) in zip(st.columns(3), [('view','浏览'),('addtocart','加购'),('transaction','购买')]):
    column.metric(label, counts[event])
st.plotly_chart(px.bar(x=list(counts), y=list(counts.values()), labels={'x':'行为','y':'事件数'}), use_container_width=True)
