"""Known events -> complete Spark batch -> public DuckDB queries (local only)."""
import json
from datetime import datetime, timezone
from retailrocket import query


def test_date_metrics_from_complete_batch(tmp_path):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    spark = SparkSession.builder.master('local[2]').appName('date-contract').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.shuffle.partitions', '2')
    entries = [('2015-05-02T23:59:59', 'outside', 'view'),
               ('2015-05-03T00:00:00', 'a', 'transaction'),
               ('2015-05-03T12:00:00', 'a', 'view'),
               ('2015-05-03T13:00:00', 'only', 'transaction'),
               ('2015-05-04T01:00:00', 'a', 'view'),
               ('2015-05-04T02:00:00', 'a', 'addtocart'),
               ('2015-05-04T23:59:59', 'b', 'view'),
               ('2015-05-05T00:00:00', 'outside', 'transaction')]
    rows = [dict(timestamp=str(int(datetime.fromisoformat(t).replace(tzinfo=timezone.utc).timestamp()*1000)), visitorid=v, event=e, itemid='1', transactionid='', source_file='events.csv', dataset_version='fixture', batch_id='fixture', source_line=i+2) for i,(t,v,e) in enumerate(entries)]
    rows.append(dict(rows[0], source_line=20, timestamp='invalid'))
    try:
        analyze(spark.createDataFrame([(json.dumps(r),) for r in rows], 'value STRING'), {'batch_id':'fixture','rows':len(rows)}, str(tmp_path))
        assert query.overview(tmp_path/'events','2015-05-03','2015-05-04') == dict(view=3,addtocart=1,transaction=2)
        assert query.visitor_conversion(tmp_path,'2015-05-03','2015-05-04') == dict(visitors=3,view_visitors=2,cart_visitors=1,purchase_visitors=2,view_purchase_visitors=1,cart_purchase_visitors=1,view_purchase_rate=.5,cart_purchase_rate=1.)
        daily = query.daily_trend(tmp_path,'2015-05-03','2015-05-04')
        assert [(str(r['day']),r['visitors'],r['view'],r['transaction']) for r in daily] == [('2015-05-03',2,1,2),('2015-05-04',2,2,0)]
        empty = query.visitor_conversion(tmp_path,'2015-06-01','2015-06-02')
        assert empty['visitors'] == 0 and empty['view_purchase_rate'] is None and empty['cart_purchase_rate'] is None
        purchase = query.visitor_conversion(tmp_path,'2015-05-05','2015-05-05')
        assert purchase['purchase_visitors'] == 1 and purchase['view_purchase_rate'] is None
        assert query.daily_trend(tmp_path,'2015-06-01','2015-06-02') == []
        assert query.range_quality(tmp_path,'2015-05-03','2015-05-04') == dict(valid_count=6,invalid_count=0)
        assert query.quality_summary(tmp_path)['invalid_count'] == 1
    finally:
        spark.stop()
