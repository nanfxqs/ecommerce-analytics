"""Range aggregates over Spark's daily category and item results."""
from pathlib import Path
import duckdb


def category_analysis(directory, start, end):
    with duckdb.connect() as conn:
        def aggregate(name, key):
            rows = conn.execute(f'''SELECT {key},sum(events)::BIGINT events,
                sum("views")::BIGINT AS "views",sum(addtocarts)::BIGINT addtocarts,
                sum(transactions)::BIGINT transactions FROM read_parquet(?)
                WHERE event_date BETWEEN cast(? AS DATE) AND cast(? AS DATE)
                GROUP BY {key} ORDER BY events DESC,{key}''',
                [str(Path(directory)/name/'*.parquet'),start,end]).fetchall()
            return [dict(zip((key,'events','views','addtocarts','transactions'), row)) for row in rows]
        categories = aggregate('category_daily','categoryid')
        items = aggregate('item_daily','itemid')
    total = sum(row['events'] for row in categories)
    unknown = sum(row['events'] for row in categories if row['categoryid']=='unknown')
    return dict(items=items,categories=categories,coverage=dict(total=total,associated=total-unknown,unknown=unknown,rate=(total-unknown)/total if total else None))
