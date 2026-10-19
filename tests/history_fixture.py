"""Known input -> Spark -> validated publication, shared by HTTP and browser tests."""
import json
from pathlib import Path
from retailrocket.ingest import prepare, records
from retailrocket.references import prepare_references
from retailrocket.publication import publish_batch
from retailrocket.validation import validate_analysis


def publish_known_history(directory):
    from pyspark.sql import SparkSession
    from retailrocket.spark_batch import analyze
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / 'events.csv'
    source.write_text('timestamp,visitorid,event,itemid,transactionid\n0,1,view,a,\n86400000,1,transaction,a,9\n86400001,1,view,b,\n86400002,2,transaction,a,10\n172799999,3,addtocart,a,\nbad,1,view,a,\n0,1,view,a,\n259200000,4,view,a,\n')
    manifest = prepare(source, directory/'prepared', 'known-v1')
    properties = directory/'properties.csv'
    properties.write_text('timestamp,itemid,property,value\n0,a,categoryid,10\n172800000,a,categoryid,99\n172800000,b,categoryid,20\n')
    tree = directory/'tree.csv'
    tree.write_text('categoryid,parentid\n10,\n99,\n20,\n')
    refs = prepare_references([properties], tree, directory/'refs', 'known-v1', manifest)
    rows = [dict(row, batch_id='known', replay_time='2026-10-19T00:00:00+00:00') for row in records(manifest)]
    replay = dict(batch_id='known', rows=9, replay_confirmed=True, input=json.loads(manifest.read_text()), topic='known', startingOffsets={'known':{'0':0}}, endingOffsets={'known':{'0':9}})
    replay_path = directory/'replay.json'
    replay_path.write_text(json.dumps(replay))
    output = directory/'output'
    spark = SparkSession.builder.master('local[2]').appName('http-history-contract').config('spark.sql.shuffle.partitions','2').getOrCreate()
    try:
        raw = spark.createDataFrame([(json.dumps(row),) for row in rows+[rows[0]]], 'value STRING')
        analyze(raw, replay, str(output), references=str(refs.parent))
        (output/'metadata.json').write_text(next((output/'metadata').glob('part-*')).read_text())
        store = directory/'published'
        publish_batch(store, replay_path, output, validate=validate_analysis)
        return store
    finally:
        spark.stop()


if __name__ == '__main__':
    import sys
    print(publish_known_history(sys.argv[1]))
