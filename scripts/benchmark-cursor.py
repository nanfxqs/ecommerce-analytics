#!/usr/bin/env python3
"""Measure actual durable cursor I/O in a private temporary directory on edge."""
import argparse
import json
from pathlib import Path
import tempfile
import time
from retailrocket.cursor_checkpoint import CursorCheckpoint
from retailrocket.edge_worker import atomic
p=argparse.ArgumentParser();p.add_argument('--records',type=int,default=1000);args=p.parse_args()
report=[]
with tempfile.TemporaryDirectory(prefix='retailrocket-cursor-audit-') as temporary:
    for batch in [1,100]:
        state=dict(cursor=0,confirmed=0,phase='running')
        destination=Path(temporary)/str(batch)
        start=time.monotonic()
        checkpoint=CursorCheckpoint(state,lambda value:atomic(destination,value),records=batch)
        for index in range(1,args.records+1):checkpoint.acknowledged(cursor=index,confirmed=index)
        checkpoint.close()
        elapsed=time.monotonic()-start
        assert json.loads(destination.read_text())['cursor']==args.records
        report.append(dict(records=args.records,flush_records=batch,seconds=elapsed,records_per_second=args.records/elapsed))
print(json.dumps(report,indent=2))
