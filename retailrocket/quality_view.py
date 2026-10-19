"""Bounded inspection of the persisted quarantine through the query interface."""
from retailrocket.query import anomaly_records, anomaly_reasons


def render_anomalies(directory, start=None, end=None):
    import streamlit as st
    st.subheader('异常原因与记录')
    options = ['整个批次（包含无法确定日期的记录）']
    if start is not None and end is not None:
        options.append('所选 UTC 日期（仅包含时间有效的记录）')
    scope = st.radio('异常查看范围', options)
    dates = (start, end) if scope == options[-1] and len(options) == 2 else (None, None)
    st.caption('每条异常记录显示首个校验失败原因；日期筛选不包含时间无效的记录。源文件与过滤前行号用于追溯原始输入。')
    reasons = anomaly_reasons(directory, *dates)
    if not reasons:
        st.info('该范围没有异常记录')
        return
    st.dataframe(reasons, hide_index=True)
    limit = st.selectbox('显示异常记录上限', [100, 250, 500])
    st.caption('按数据版本、源文件、源行号排序显示前若干条；原因统计覆盖该范围全部异常。')
    st.dataframe(anomaly_records(directory, *dates, limit=limit), hide_index=True)
