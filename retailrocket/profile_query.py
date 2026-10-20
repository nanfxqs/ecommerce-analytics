"""Visitor profiles from published Spark summaries, with UTC date filtering."""
from pathlib import Path
import duckdb


def visitor_profiles(directory, start, end, visitorid=None, page=1, page_size=20, top_n=5):
    if page < 1 or not 1 <= page_size <= 100 or not 1 <= top_n <= 50:
        raise ValueError('Invalid profile pagination or category limit')
    with duckdb.connect() as conn:
        conn.execute('''CREATE TEMP TABLE activity AS SELECT visitorid,
            count(*)::BIGINT active_days, sum(events)::BIGINT events,
            sum(purchases)::BIGINT purchases, max(last_event_time) last_event_time
            FROM read_parquet(?) WHERE event_date BETWEEN cast(? AS DATE) AND cast(? AS DATE)
            GROUP BY visitorid''', [str(Path(directory)/'visitor_daily'/'*.parquet'), start, end])
        reference = conn.execute('SELECT max(last_event_time) FROM activity').fetchone()[0]
        # Population charts describe the entire date range, never a page or ID lookup.
        distribution = conn.execute('''WITH population AS (
            SELECT *,epoch(?::TIMESTAMP-last_event_time) recency FROM activity)
            SELECT count(*),
                count(*) FILTER (WHERE active_days=1),
                count(*) FILTER (WHERE active_days BETWEEN 2 AND 3),
                count(*) FILTER (WHERE active_days BETWEEN 4 AND 7),
                count(*) FILTER (WHERE active_days>=8),
                count(*) FILTER (WHERE recency<86400),
                count(*) FILTER (WHERE recency>=86400 AND recency<604800),
                count(*) FILTER (WHERE recency>=604800 AND recency<2592000),
                count(*) FILTER (WHERE recency>=2592000)
            FROM population''', [reference]).fetchone()
        population_total = distribution[0]
        def buckets(labels, counts):
            return [dict(label=label,count=count,share=count/population_total if population_total else None)
                    for label,count in zip(labels,counts)]
        population = dict(total=population_total,scope='selected_range_all_visitors',
            active_days=buckets(['1天','2～3天','4～7天','8天及以上'],distribution[1:5]),
            recency=buckets(['不足1天','1～不足7天','7～不足30天','30天及以上'],distribution[5:9]))
        total = conn.execute('SELECT count(*) FROM activity WHERE (? IS NULL OR visitorid=?)', [visitorid, visitorid]).fetchone()[0]
        rows = conn.execute('''SELECT *, epoch(?::TIMESTAMP-last_event_time) recency_seconds
            FROM activity WHERE (? IS NULL OR visitorid=?) ORDER BY visitorid
            LIMIT ? OFFSET ?''', [reference,visitorid,visitorid,page_size,(page-1)*page_size]).fetchall()
        visitors=[dict(zip(('visitorid','active_days','events','purchases','last_event_time','recency_seconds'),row), categories=[]) for row in rows]
        if visitors:
            placeholders = ','.join('?' for _ in visitors)
            categories = conn.execute(f'''SELECT visitorid, categoryid,
                sum("views")::BIGINT AS "views", sum(purchases)::BIGINT purchases
                FROM read_parquet(?) WHERE event_date BETWEEN cast(? AS DATE) AND cast(? AS DATE)
                AND visitorid IN ({placeholders}) GROUP BY visitorid,categoryid
                HAVING sum("views")>0
                QUALIFY row_number() OVER (PARTITION BY visitorid ORDER BY sum("views") DESC, categoryid)<=?
                ORDER BY visitorid,"views" DESC,categoryid''',
                [str(Path(directory)/'visitor_category_daily'/'*.parquet'),start,end,
                 *[visitor['visitorid'] for visitor in visitors],top_n]).fetchall()
            by_id={visitor['visitorid']:visitor for visitor in visitors}
            for visitorid,categoryid,views,purchases in categories:
                by_id[visitorid]['categories'].append(dict(categoryid=categoryid,views=views,purchases=purchases))
    return dict(total=total,page=page,page_size=page_size,reference_time=reference,visitors=visitors,population=population)
