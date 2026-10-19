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
    return dict(total=total,page=page,page_size=page_size,reference_time=reference,visitors=visitors)
