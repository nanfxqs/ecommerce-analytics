"""Retailrocket result contract, separate from generic atomic publication."""
import json
import re
import duckdb


def validate_analysis(snapshot):
    """Reject partial exports or mismatched provenance before the pointer moves."""
    root, metadata = snapshot.directory, snapshot.metadata
    replay = json.loads((root / 'publication.json').read_text())['replay']
    source = replay.get('input', {})
    if metadata.get('input') != source:
        raise ValueError('Export input provenance differs from confirmed replay')
    required_source = ('dataset_version', 'source_file', 'source_sha256', 'rows', 'first_source_line', 'last_source_line')
    if any(key not in source for key in required_source) or not re.fullmatch('[0-9a-f]{64}', source.get('source_sha256', '')):
        raise ValueError('Input source provenance is incomplete')
    refs = metadata.get('references')
    if not refs or refs.get('events_input') != source or refs.get('dataset_version') != source['dataset_version']:
        raise ValueError('Reference provenance differs from event input')
    if not refs.get('sources') or any(not entry.get('file') or not re.fullmatch('[0-9a-f]{64}', entry.get('sha256', '')) or 'rows' not in entry for entry in refs['sources']):
        raise ValueError('Reference source provenance is incomplete')
    tables = ('raw', 'events', 'quarantine', 'quality', 'overview', 'visitor_events',
              'category_daily', 'item_daily', 'visitor_daily', 'visitor_category_daily')
    with duckdb.connect() as db:
        for name in tables:
            paths = sorted((root / name).glob('*.parquet'))
            if not paths:
                raise ValueError('Incomplete analysis export: missing ' + name)
            db.read_parquet([str(path) for path in paths]).create_view(name)
        rows = db.execute('SELECT * FROM quality').fetchall()
        columns = [column[0] for column in db.description]
        if len(rows) != 1:
            raise ValueError('quality must contain exactly one summary')
        quality = dict(zip(columns, rows[0]))
        if quality != metadata.get('quality'):
            raise ValueError('quality metadata differs from persisted summary')
        counts = {name: db.execute('SELECT count(*) FROM "'+name+'"').fetchone()[0] for name in ('raw','events','quarantine')}
        expected = dict(input_count=source['rows'], consumed_count=replay['rows'],
                        unique_source_count=source['rows'], valid_count=counts['events'],
                        invalid_count=counts['quarantine'], retry_count=counts['raw']-source['rows'])
        if counts['raw'] != replay['rows'] or any(quality.get(k) != v for k,v in expected.items()) or counts['events']+counts['quarantine'] != source['rows']:
            raise ValueError('quality counts do not reconcile input, replay and persisted results')
        union = '(SELECT * FROM events UNION ALL BY NAME SELECT * FROM quarantine)'
        n, unique, minimum, maximum, wrong = db.execute('SELECT count(*), count(DISTINCT source_line), min(source_line), max(source_line), count(*) FILTER (WHERE dataset_version IS DISTINCT FROM ? OR source_file IS DISTINCT FROM ? OR batch_id IS DISTINCT FROM ?) FROM '+union, [source['dataset_version'], source['source_file'], snapshot.batch_id]).fetchone()
        if wrong or n != unique or n != source['rows'] or (n and (minimum != source['first_source_line'] or maximum != source['last_source_line'] or maximum-minimum+1 != n)):
            raise ValueError('Source completeness or identity check failed')
        # Compare multisets, not only gross totals: missing keys, duplicates and
        # wrong dimensions must not publish plausible-looking partial dashboards.
        summaries = {
            'overview': ('event,count', 'SELECT event,count(*) AS count FROM events GROUP BY event'),
            'visitor_events': ('event_date,visitorid,event', 'SELECT DISTINCT cast(event_time AS DATE) AS event_date,visitorid,event FROM events'),
            'visitor_daily': ('event_date,visitorid,events,purchases,last_event_time',
                "SELECT cast(event_time AS DATE) AS event_date,visitorid,count(*) AS events,count(*) FILTER (WHERE event='transaction') AS purchases,max(event_time) AS last_event_time FROM events GROUP BY event_date,visitorid"),
            'visitor_category_daily': ('event_date,visitorid,categoryid,views,purchases',
                "SELECT cast(event_time AS DATE) AS event_date,visitorid,categoryid,count(*) FILTER (WHERE event='view') AS views,count(*) FILTER (WHERE event='transaction') AS purchases FROM events GROUP BY event_date,visitorid,categoryid"),
        }
        for dimension, table in [('categoryid', 'category_daily'), ('itemid', 'item_daily')]:
            columns = 'event_date,'+dimension+',events,views,addtocarts,transactions'
            expected_sql = ("SELECT cast(event_time AS DATE) AS event_date,"+dimension+",count(*) AS events,"
                            "count(*) FILTER (WHERE event='view') AS views,"
                            "count(*) FILTER (WHERE event='addtocart') AS addtocarts,"
                            "count(*) FILTER (WHERE event='transaction') AS transactions "
                            "FROM events GROUP BY event_date,"+dimension)
            summaries[table] = columns, expected_sql
        for table, (columns, expected_sql) in summaries.items():
            db.execute('CREATE OR REPLACE TEMP VIEW expected_result AS '+expected_sql)
            actual = 'SELECT '+columns+' FROM '+table
            expected_result = 'SELECT '+columns+' FROM expected_result'
            differences = '(('+actual+') EXCEPT ALL ('+expected_result+')) UNION ALL (('+expected_result+') EXCEPT ALL ('+actual+'))'
            if db.execute('SELECT EXISTS(SELECT 1 FROM ('+differences+'))').fetchone()[0]:
                raise ValueError(table+' does not reconcile with authoritative events')
