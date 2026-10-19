"""A complete batch needs Python on the driver, not on executor hosts."""
import json
import os
import subprocess
import sys


def test_batch_publishes_result_files_without_executor_python(tmp_path):
    source = tmp_path/'messages.jsonl'
    source.write_text(json.dumps(dict(timestamp='1430622000000',visitorid='1',itemid='2',event='view',transactionid='',source_record_id='known',source_file='events.csv',source_line=2,dataset_version='known',batch_id='known'))+'\n')
    script = '''
import sys
from pyspark.sql import SparkSession
from retailrocket.spark_batch import analyze
spark = SparkSession.builder.master('local[2]').appName('no-worker-python').config('spark.sql.shuffle.partitions','2').getOrCreate()
try:
    result = analyze(spark.read.text(sys.argv[1]), {'batch_id':'known','rows':1,'input':{'rows':1}}, sys.argv[2])
    assert result['valid_count']==1
finally:
    spark.stop()
'''
    result = subprocess.run([sys.executable,'-c',script,str(source),str(tmp_path/'output')],env={**os.environ,'PYSPARK_PYTHON':'/intentionally-missing-executor-python'},capture_output=True,text=True)
    assert result.returncode == 0, result.stderr[-7000:]
    from retailrocket.query import overview
    assert overview(tmp_path/'output'/'events','2015-05-03','2015-05-03') == {'view':1,'addtocart':0,'transaction':0}
    assert len(list((tmp_path/'output'/'metadata').glob('part-*'))) == 1
