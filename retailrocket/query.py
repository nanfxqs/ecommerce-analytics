"""Read a single exported historical batch through DuckDB."""
from pathlib import Path
import duckdb


def overview(directory, start, end):
    start, end = _range(start, end)
    path = str(Path(directory) / '*.parquet')
    with duckdb.connect() as connection:
        rows = connection.execute("SELECT event,count(*) FROM read_parquet(?) WHERE cast(event_time AS DATE) BETWEEN cast(? AS DATE) AND cast(? AS DATE) GROUP BY event", [path, start, end]).fetchall()
    result = dict.fromkeys(('view', 'addtocart', 'transaction'), 0)
    result.update(dict(rows))
    return result


def quality_summary(directory):
    """Return batch-wide reconciliation; suspected duplicates overlap valid/invalid."""
    path = str(Path(directory) / 'quality' / '*.parquet')
    with duckdb.connect() as connection:
        row = connection.execute('SELECT * FROM read_parquet(?)', [path])
        names = [column[0] for column in row.description]
        values = row.fetchall()
    if len(values) != 1:
        raise ValueError('Expected one batch quality summary')
    return dict(zip(names, values[0]))


def _path(directory, table):
    return str(Path(directory) / table / '*.parquet')


def _range(start, end):
    from datetime import date
    start, end = date.fromisoformat(str(start)), date.fromisoformat(str(end))
    if start > end:
        raise ValueError('Start date must not follow end date')
    return start, end


def date_bounds(directory):
    with duckdb.connect() as connection:
        return connection.execute('SELECT min(cast(event_time AS DATE)), max(cast(event_time AS DATE)) FROM read_parquet(?)', [_path(directory, 'events')]).fetchone()


def visitor_conversion(directory, start, end):
    """Distinct visitors and unordered purchase intersections over inclusive UTC days."""
    start, end = _range(start, end)
    with duckdb.connect() as connection:
        row = connection.execute('''WITH visitors AS (
            SELECT visitorid, bool_or(event='view') AS viewed,
                bool_or(event='addtocart') AS carted, bool_or(event='transaction') AS purchased
            FROM read_parquet(?) WHERE event_date BETWEEN ? AND ? GROUP BY visitorid)
            SELECT count(*), count(*) FILTER (WHERE viewed), count(*) FILTER (WHERE carted),
                count(*) FILTER (WHERE purchased), count(*) FILTER (WHERE viewed AND purchased),
                count(*) FILTER (WHERE carted AND purchased) FROM visitors''',
            [_path(directory, 'visitor_events'), start, end]).fetchone()
    names = ['visitors', 'view_visitors', 'cart_visitors', 'purchase_visitors', 'view_purchase_visitors', 'cart_purchase_visitors']
    result = dict(zip(names, row))
    result['view_purchase_rate'] = row[4] / row[1] if row[1] else None
    result['cart_purchase_rate'] = row[5] / row[2] if row[2] else None
    return result


def daily_trend(directory, start, end):
    start, end = _range(start, end)
    with duckdb.connect() as connection:
        result = connection.execute('''SELECT cast(event_time AS DATE) AS day,
            count(*) FILTER (WHERE event='view') AS view,
            count(*) FILTER (WHERE event='addtocart') AS addtocart,
            count(*) FILTER (WHERE event='transaction') AS transaction,
            count(DISTINCT visitorid) AS visitors FROM read_parquet(?)
            WHERE cast(event_time AS DATE) BETWEEN ? AND ? GROUP BY day ORDER BY day''',
            [_path(directory, 'events'), start, end])
        columns = [c[0] for c in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]


def range_quality(directory, start, end):
    """Only timestamped rows can be attributed to selected UTC days."""
    start, end = _range(start, end)
    counts = {}
    with duckdb.connect() as connection:
        for table, name in [('events', 'valid_count'), ('quarantine', 'invalid_count')]:
            counts[name] = connection.execute('SELECT count(*) FROM read_parquet(?) WHERE cast(event_time AS DATE) BETWEEN ? AND ?', [_path(directory, table), start, end]).fetchone()[0]
    return counts


def _anomaly_scope(start, end):
    if start is None and end is None:
        return '', []
    if start is None or end is None:
        raise ValueError('Both UTC dates are required')
    start, end = _range(start, end)
    return ' WHERE cast(event_time AS DATE) BETWEEN ? AND ?', [start, end]


def anomaly_reasons(directory, start=None, end=None):
    """Counts by primary quarantine reason; omit dates to include invalid times."""
    scope, dates = _anomaly_scope(start, end)
    with duckdb.connect() as connection:
        rows = connection.execute('SELECT invalid_reason, count(*) FROM read_parquet(?)' + scope +
                                  ' GROUP BY invalid_reason ORDER BY invalid_reason',
                                  [_path(directory, 'quarantine'), *dates]).fetchall()
    return [dict(invalid_reason=reason, count=count) for reason, count in rows]


def anomaly_records(directory, start=None, end=None, limit=100):
    """A stable bounded sample of original fields and their quarantine reason."""
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 500:
        raise ValueError('Anomaly record limit must be between 1 and 500')
    scope, dates = _anomaly_scope(start, end)
    with duckdb.connect() as connection:
        result = connection.execute('''SELECT dataset_version, source_file, source_line,
            timestamp, event_time, visitorid, event, itemid, transactionid, invalid_reason
            FROM read_parquet(?)''' + scope +
            ' ORDER BY dataset_version, source_file, source_line LIMIT ?',
            [_path(directory, 'quarantine'), *dates, limit])
        names = [column[0] for column in result.description]
        return [dict(zip(names, row)) for row in result.fetchall()]
