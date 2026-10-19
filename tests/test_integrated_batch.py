"""Approved seam: fixed inputs/references -> published snapshot -> dashboard queries.

Local Spark verifies semantics only. Actual distributed evidence lives in docs/evidence.
"""
import json
from pathlib import Path
import pytest
from retailrocket.ingest import prepare, records
from retailrocket.references import prepare_references
from retailrocket.publication import publish_batch, current_snapshot
from retailrocket.validation import validate_analysis
from retailrocket.query import overview, visitor_conversion
from retailrocket.profile_query import visitor_profiles


def test_integrated_batch_preserves_semantics_across_failure_and_raw_retry(tmp_path, monkeypatch):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    source = tmp_path / 'events.csv'
    source.write_text('timestamp,visitorid,event,itemid,transactionid\n0,1,view,a,\n86400000,1,transaction,a,9\n86400001,1,view,b,\n86400002,2,transaction,a,10\n172799999,3,addtocart,a,\nbad,1,view,a,\n0,1,view,a,\n259200000,4,view,a,\n')
    manifest = prepare(source, tmp_path/'prepared', 'known-v1')
    properties = tmp_path/'properties.csv'
    properties.write_text('timestamp,itemid,property,value\n0,a,categoryid,10\n172800000,a,categoryid,99\n172800000,b,categoryid,20\n')
    tree = tmp_path/'tree.csv'
    tree.write_text('categoryid,parentid\n10,\n99,\n20,\n')
    refs = prepare_references([properties], tree, tmp_path/'refs', 'known-v1', manifest)
    source_rows = list(records(manifest))
    for row in source_rows:
        row.update(batch_id='known', replay_time='2026-10-19T00:00:00+00:00')
    replay = dict(batch_id='known', rows=9, replay_confirmed=True, input=json.loads(manifest.read_text()), topic='known', startingOffsets={'known':{'0':0}}, endingOffsets={'known':{'0':9}})
    replay_path = tmp_path/'replay.json'
    replay_path.write_text(json.dumps(replay))
    output = tmp_path/'output'
    store = tmp_path/'published'
    spark = SparkSession.builder.master('local[2]').appName('integrated-contract').config('spark.sql.shuffle.partitions','2').getOrCreate()
    try:
        raw = spark.createDataFrame([(json.dumps(row),) for row in source_rows+[source_rows[0]]], 'value STRING')
        quality = analyze(raw, replay, str(output), references=str(refs.parent))
        assert (quality['consumed_count'],quality['unique_source_count'],quality['valid_count'],quality['invalid_count'],quality['suspected_duplicate_count']) == (9,8,7,1,1)
        (output/'metadata.json').write_text(next((output/'metadata').glob('part-*')).read_text())
        initial = publish_batch(store,replay_path,output,validate=validate_analysis)
        assert overview(initial.directory/'events','1970-01-01','1970-01-02') == dict(view=3,addtocart=1,transaction=2)
        conversions = visitor_conversion(initial.directory,'1970-01-01','1970-01-02')
        assert (conversions['visitors'],conversions['view_purchase_visitors'],conversions['view_purchase_rate'],conversions['cart_purchase_rate']) == (3,1,1.0,0.0)
        profile = visitor_profiles(initial.directory,'1970-01-01','1970-01-02',visitorid='1')['visitors'][0]
        assert (profile['active_days'],profile['events'],profile['purchases'],profile['recency_seconds']) == (2,4,1,86399.998)
        assert profile['categories'] == [dict(categoryid='10',views=2,purchases=1),dict(categoryid='unknown',views=1,purchases=0)]
        assert visitor_conversion(initial.directory,'1970-01-03','1970-01-03')['view_purchase_rate'] is None
        assert visitor_profiles(initial.directory,'1970-01-01','1970-01-02',visitorid='missing')['total'] == 0
        analyze(spark.read.parquet(str(output/'raw')),replay,str(output),reuse_raw=True,references=str(refs.parent))
        (output/'metadata.json').write_text(next((output/'metadata').glob('part-*')).read_text())
        def injected_failure(snapshot):
            raise RuntimeError('controlled failure before pointer switch')
        with pytest.raises(RuntimeError,match='controlled failure'):
            publish_batch(store,replay_path,output,validate=validate_analysis,before_switch=injected_failure)
        assert current_snapshot(store).directory == initial.directory
        retried = publish_batch(store,replay_path,output,validate=validate_analysis)
        assert overview(retried.directory/'events','1970-01-01','1970-01-02') == dict(view=3,addtocart=1,transaction=2)
        import sys
        from datetime import date
        from streamlit.testing.v1 import AppTest
        monkeypatch.setattr(sys, 'argv', ['dashboard.py', str(store)])
        app = AppTest.from_file(str(Path(__file__).parents[1]/'retailrocket/dashboard.py')).run(timeout=30)
        assert not app.exception
        app.date_input[0].set_value((date(1970,1,1),date(1970,1,1))).run(timeout=30)
        assert {metric.label:metric.value for metric in app.metric} == {'浏览事件':'2','加购事件':'0','购买事件':'0','独立访客':'1'}
        for page in ['概览','购买关联','分类偏好','用户画像','数据质量']:
            app.sidebar.radio[0].set_value(page).run(timeout=30)
            assert not app.exception, (page, app.exception)
            assert app.date_input[0].value == (date(1970,1,1),date(1970,1,1))
        app.sidebar.radio[0].set_value('用户画像').run(timeout=30)
        app.text_input[0].set_value('missing-visitor').run(timeout=30)
        assert any('无访客数据' in message.value for message in app.info)
        app.text_input[0].set_value('').run(timeout=30)
        app.number_input[0].set_value(999).run(timeout=30)
        assert any('无访客数据' in message.value for message in app.info)

        # Jan 3 lies inside the batch bounds (Jan 1..4), with no events.
        app.date_input[0].set_value((date(1970,1,3),date(1970,1,3))).run(timeout=30)
        for page in ['概览','购买关联','分类偏好','用户画像','数据质量']:
            app.sidebar.radio[0].set_value(page).run(timeout=30)
            assert not app.exception, (page, app.exception)
            assert app.date_input[0].value == (date(1970,1,3),date(1970,1,3))
            if page == '概览':
                assert all(metric.value == '0' for metric in app.metric)
                assert any('无数据' in message.value for message in app.info)
            elif page == '购买关联':
                assert sum(metric.value == '无数据' for metric in app.metric) == 2
    finally:
        spark.stop()
