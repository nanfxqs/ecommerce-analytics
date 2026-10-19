#!/usr/bin/env python3
"""Run on the edge node: real Kafka/YARN/HDFS known-input restart audit.

Use a NEW --run-id; never writes existing topics or modifies cluster config.
The two submissions deliberately share a checkpoint and output. Requires the
same isolated runtime environment as submit-stream.sh and kafka-python.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--brokers', default='hadoop01:9092,hadoop02:9092,hadoop03:9092')
    p.add_argument('--run-id', required=True)
    p.add_argument('--work', type=Path, required=True)
    args = p.parse_args()
    args.work.mkdir(parents=True, exist_ok=False)
    topic = args.run_id
    admin = KafkaAdminClient(bootstrap_servers=args.brokers.split(','))
    try:
        admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=3)])
    finally:
        admin.close()
    producer = KafkaProducer(bootstrap_servers=args.brokers.split(','), acks='all',
                             value_serializer=lambda row:json.dumps(row).encode())
    def row(i):
        return dict(dataset_version='v2-feasibility', source_file='fixture.csv', source_line=i+2,
                    batch_id=args.run_id, timestamp='bad' if i>=990 else str((999-i)*86400000),
                    visitorid=str(i%25), event='view', itemid='a')
    reports = []
    try:
        for phase, rows in [('first', [row(i) for i in range(500)]+[row(i) for i in range(10)]),
                            ('resumed', [row(i) for i in range(500,1000)]+[dict(row(0),source_line=1002)])]:
            started = time.monotonic()
            for i, item in enumerate(rows):
                producer.send(topic, value=item, partition=i%3).get(timeout=30)
            producer.flush()
            report = dict(phase=phase, sent=len(rows), send_seconds=time.monotonic()-started)
            evidence = args.work/(phase+'-evidence.json')
            command = [str(Path(__file__).with_name('submit-stream.sh')), '--brokers', args.brokers,
                       '--topic', topic, '--run-id', args.run_id, '--output',
                       'hdfs://mycluster/retailrocket/streams/'+args.run_id, '--checkpoint',
                       'hdfs://mycluster/retailrocket/stream-checkpoints/'+args.run_id,
                       '--available-now', '--evidence', str(evidence)]
            with (args.work/(phase+'.log')).open('w') as log:
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
            result = json.loads(evidence.read_text())
            report['evidence'] = result
            reports.append(report)
            snapshot = result['snapshot']
            assert snapshot['master'] == 'yarn'
            expected = (dict(consumed_count=510, unique_source_count=500, valid_count=500,
                             invalid_count=0, retry_count=10, suspected_duplicate_count=0) if phase=='first' else
                        dict(consumed_count=1011, unique_source_count=1001, valid_count=991,
                             invalid_count=10, retry_count=10, suspected_duplicate_count=1))
            assert snapshot['quality'] == expected, (snapshot['quality'], expected)
            assert snapshot['visitors'] == 25
            assert snapshot['overview'] == {'view': expected['valid_count']}
    finally:
        producer.close()
    (args.work/'audit.json').write_text(json.dumps(reports, indent=2))
    print(json.dumps(dict(status='passed', audit=str(args.work/'audit.json'))))


if __name__ == '__main__':
    main()
