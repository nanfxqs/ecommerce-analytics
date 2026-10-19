"""Known batch -> Spark summaries -> range-filtered visitor profiles."""
import json


def test_profiles_use_range_activity_and_stable_view_preferences(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    from retailrocket.profile_query import visitor_profiles
    spark = SparkSession.builder.master('local[2]').appName('profile-contract').getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    refs = tmp_path/'refs'
    refs.mkdir()
    (refs/'categories.csv').write_text('timestamp,itemid,categoryid\n0,a,10\n0,b,20\n')
    (refs/'references.json').write_text(json.dumps(dict(dataset_version='v1',properties='categories.csv')))
    rows = [('1',0,'a','view'),('1',86400000,'b','view'),('1',86400001,'a','transaction'),('2',172799999,'a','view'),('3',172800000,'a','view')]
    messages = [json.dumps(dict(visitorid=v,timestamp=str(t),itemid=i,event=e,source_file='events.csv',source_line=n+2,dataset_version='v1',batch_id='fixture')) for n,(v,t,i,e) in enumerate(rows)]
    try:
        analyze(spark.createDataFrame([(x,) for x in messages], 'value STRING'),dict(batch_id='fixture',rows=5,input=dict(dataset_version='v1')),str(tmp_path/'batch'),references=str(refs))
        result = visitor_profiles(tmp_path/'batch','1970-01-01','1970-01-02',page_size=1,top_n=1)
        assert result['total'] == 2
        first = result['visitors'][0]
        assert (first['visitorid'],first['active_days'],first['events'],first['purchases']) == ('1',2,3,1)
        assert first['recency_seconds'] == 86399.998
        assert first['categories'] == [dict(categoryid='10',views=1,purchases=1)]
        assert visitor_profiles(tmp_path/'batch','1970-01-01','1970-01-02',page=2,page_size=1)['visitors'][0]['visitorid']=='2'
        single=visitor_profiles(tmp_path/'batch','1970-01-02','1970-01-02',visitorid='1')['visitors'][0]
        assert (single['active_days'],single['events'],single['purchases']) == (1,2,1)
        assert single['categories']==[dict(categoryid='20',views=1,purchases=0)]
        assert visitor_profiles(tmp_path/'batch','1970-01-01','1970-01-02',visitorid='1 OR 1=1')['total']==0
        assert visitor_profiles(tmp_path/'batch','1970-01-04','1970-01-04')['visitors']==[]
    finally:
        spark.stop()
