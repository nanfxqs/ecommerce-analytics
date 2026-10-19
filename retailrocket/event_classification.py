"""Shared Spark SQL event classification; aggregation stays with each pipeline."""
from pyspark.sql import functions as F


def classify_events(rows):
    """Add guarded UTC event time and the shared validation priority."""
    # Guard before multiplying millis into Spark's microsecond representation.
    events = rows.withColumn('event_time', F.expr('''CASE
        WHEN try_cast(timestamp AS bigint) BETWEEN -62135596800000 AND 253402300799999
        THEN timestamp_millis(try_cast(timestamp AS bigint)) END'''))
    return events.withColumn('invalid_reason',
        F.when(F.col('event_time').isNull(), 'invalid_timestamp')
        .when(F.col('visitorid').isNull() | (F.trim('visitorid') == ''), 'missing_visitor')
        .when(F.col('itemid').isNull() | (F.trim('itemid') == ''), 'missing_item')
        .when(F.col('event').isNull() | ~F.col('event').isin('view', 'addtocart', 'transaction'), 'invalid_event'))
