"""Read a single exported historical batch through DuckDB."""
from pathlib import Path
import duckdb


def overview(directory, start, end):
    path = str(Path(directory) / '**' / '*.parquet')
    with duckdb.connect() as connection:
        rows = connection.execute("SELECT event,count(*) FROM read_parquet(?) WHERE cast(event_time AS DATE) BETWEEN cast(? AS DATE) AND cast(? AS DATE) GROUP BY event", [path, start, end]).fetchall()
    result = dict.fromkeys(('view', 'addtocart', 'transaction'), 0)
    result.update(dict(rows))
    return result
