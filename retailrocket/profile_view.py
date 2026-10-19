"""Date-scoped visitor profile page."""
import streamlit as st
from retailrocket.profile_query import visitor_profiles


def render(directory, start, end):
    st.caption('访客仅表示 visitorid 标识的行为主体，不代表注册会员或自然人数。')
    visitorid = st.text_input('访客 ID（精确查询）').strip() or None
    page_size = st.selectbox('每页访客数', [20, 50, 100])
    page = int(st.number_input('页码', min_value=1, value=1, step=1))
    top_n = st.slider('浏览偏好分类数', 1, 20, 5)
    result = visitor_profiles(directory, start, end, visitorid, page, page_size, top_n)
    st.caption(f"范围内活跃访客 {result['total']} · 最近活跃间隔基准：{result['reference_time']} UTC")
    if not result['visitors']:
        st.info('当前查询或页码无访客数据')
        return
    st.dataframe([{key:value for key,value in visitor.items() if key != 'categories'} for visitor in result['visitors']], column_config={
        'visitorid':'访客 ID', 'active_days':'活跃天数', 'events':'事件次数',
        'purchases':'购买事件次数', 'last_event_time':'最近活跃（UTC）', 'recency_seconds':'最近活跃间隔（秒）'}, hide_index=True)
    for visitor in result['visitors']:
        with st.expander(f"访客 {visitor['visitorid']} · 分类浏览偏好"):
            st.caption('按浏览次数降序；同次数按分类 ID 排序。购买事件次数单独展示，不计入偏好得分。unknown 表示无可用历史分类。')
            if visitor['categories']:
                st.dataframe(visitor['categories'], column_config={'categoryid':'分类 ID','views':'浏览次数','purchases':'该分类购买次数'}, hide_index=True)
            else:
                st.info('无浏览分类偏好')
