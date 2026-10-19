"""Persist bounded daily visitor summaries on Spark executors."""
from pyspark.sql import functions as F


def write_visitor_profiles(events, output):
    daily = events.withColumn('event_date', F.to_date('event_time'))
    daily.groupBy('event_date', 'visitorid').agg(
        F.count('*').alias('events'),
        F.sum(F.when(F.col('event') == 'transaction', 1).otherwise(0)).alias('purchases'),
        F.max('event_time').alias('last_event_time'),
    ).write.mode('overwrite').parquet(output + '/visitor_daily')
    daily.groupBy('event_date', 'visitorid', 'categoryid').agg(
        F.sum(F.when(F.col('event') == 'view', 1).otherwise(0)).alias('views'),
        F.sum(F.when(F.col('event') == 'transaction', 1).otherwise(0)).alias('purchases'),
    ).write.mode('overwrite').parquet(output + '/visitor_category_daily')
