"""Display anonymous identifiers and explicit missing-category coverage."""
from retailrocket.category_query import category_analysis


def render(root, start, end):
    import streamlit as st
    result = category_analysis(root,start,end)
    st.subheader('商品与分类分析')
    coverage = result['coverage']
    if not coverage['total']:
        st.info('所选日期范围无商品分类数据')
        return
    st.write(f"分类关联覆盖率：{coverage['rate']:.2%}；未知分类：{coverage['unknown']} / {coverage['total']} 条行为")
    st.caption('分类展示匿名原始编号；unknown 表示行为发生时尚无可用分类。')
    st.write('热门商品')
    st.dataframe(result['items'][:100],use_container_width=True)
    st.write('热门分类与分类购买情况')
    st.dataframe(result['categories'],use_container_width=True)
