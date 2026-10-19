"""Known events and changing references -> batch -> date-filtered category query."""
import json


def test_categories_use_history_without_future_fill(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    from retailrocket.category_query import category_analysis
    refs = tmp_path / 'references'
    refs.mkdir()
    (refs / 'categories.csv').write_text('timestamp,itemid,categoryid\n1000,1,10\n3000,1,20\n5000,2,30\n')
    (refs / 'category_tree.csv').write_text('categoryid,parentid\n10,\n20,10\n30,\n')
    (refs / 'references.json').write_text(json.dumps({'dataset_version':'v1','properties':'categories.csv','tree':'category_tree.csv','sources':[]}))
    spark = SparkSession.builder.master('local[2]').appName('category-contract').getOrCreate()
    spark.conf.set('spark.sql.shuffle.partitions','2')
    base = dict(visitorid='1',event='view',transactionid='',source_file='events.csv',dataset_version='v1',batch_id='fixture')
    messages = [dict(base,timestamp=str(t),itemid=item,source_line=i+2) for i,(t,item) in enumerate([(2000,'1'),(4000,'1'),(4000,'2'),(4000,'3')])]
    messages[1]['event']='transaction'
    try:
        analyze(spark.createDataFrame([(json.dumps(x),) for x in messages],'value STRING'),{'batch_id':'fixture','rows':4,'input':{'dataset_version':'v1'}},str(tmp_path/'batch'),references=str(refs))
        result=category_analysis(tmp_path/'batch','1970-01-01','1970-01-01')
        assert result['coverage']=={'total':4,'associated':2,'unknown':2,'rate':0.5}
        assert {x['categoryid']:x['events'] for x in result['categories']} == {'10':1,'20':1,'unknown':2}
        assert next(x for x in result['categories'] if x['categoryid']=='20')['transactions']==1
        assert category_analysis(tmp_path/'batch','1970-01-02','1970-01-02')['coverage']['total']==0
    finally:
        spark.stop()


def test_reference_preparation_registers_sources_and_filters_unused_properties(tmp_path):
    from retailrocket.references import prepare_references
    import hashlib
    events = tmp_path/'input.json'
    events.write_text(json.dumps({'dataset_version':'v1','source_sha256':'event-digest','rows':4}))
    props=tmp_path/'props.csv'
    props.write_text('timestamp,itemid,property,value\n1000,1,categoryid,10\n1000,1,available,1\n')
    tree=tmp_path/'tree.csv'
    tree.write_text('categoryid,parentid\n10,\n')
    manifest=prepare_references([props],tree,tmp_path/'refs','v1',events)
    result=json.loads(manifest.read_text())
    assert result['sources'][0] == dict(file='props.csv',sha256=hashlib.sha256(props.read_bytes()).hexdigest(),rows=2,category_rows=1)
    assert result['events_input']['source_sha256']=='event-digest'
    assert (manifest.parent/'categories.csv').read_text().splitlines()==['timestamp,itemid,categoryid','1000,1,10']
    assert (manifest.parent/'category_tree.csv').read_bytes()==tree.read_bytes()
