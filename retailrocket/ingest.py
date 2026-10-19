"""Immutable input provenance and acknowledged Kafka replay."""
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def prepare(source, destination, version, limit=None):
    source, destination = Path(source), Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    digest = hashlib.sha256()
    with source.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    checksum = digest.hexdigest()
    count = 0
    with source.open(newline='') as stream, (destination / 'events.jsonl').open('w') as output:
        for line, row in enumerate(csv.DictReader(stream), 2):
            if limit is not None and count >= limit:
                break
            row.update(source_line=line, source_file=source.name, dataset_version=version,
                       source_record_id=hashlib.sha256(f'{version}:{checksum}:{source.name}:{line}'.encode()).hexdigest())
            output.write(json.dumps(row) + '\n')
            count += 1
    manifest = dict(dataset_version=version, source_file=source.name, source_sha256=checksum,
                    rows=count, first_source_line=2 if count else None, last_source_line=count + 1 if count else None,
                    events='events.jsonl', mode='historical_replay', timezone='UTC')
    path = destination / 'input.json'
    path.write_text(json.dumps(manifest, indent=2))
    return path


def records(manifest):
    path = Path(manifest)
    metadata = json.loads(path.read_text())
    with (path.parent / metadata['events']).open() as stream:
        for line in stream:
            yield json.loads(line)


def replay(manifest, brokers, topic, batch_id, output):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=brokers, enable_auto_commit=False)
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise ValueError('Topic must exist before replay')
    assigned = [TopicPartition(topic, p) for p in sorted(partitions)]
    start = consumer.end_offsets(assigned)
    producer = KafkaProducer(bootstrap_servers=brokers, acks='all', retries=10,
                             value_serializer=lambda value: json.dumps(value).encode())
    count = 0
    try:
        for row in records(manifest):
            row.update(batch_id=batch_id, replay_time=datetime.now(timezone.utc).isoformat())
            producer.send(topic, key=row['source_record_id'].encode(), value=row).get(timeout=120)
            count += 1
        producer.flush(timeout=120)
        end = consumer.end_offsets(assigned)
        result = dict(batch_id=batch_id, topic=topic, rows=count, replay_confirmed=True,
                      startingOffsets={topic: {str(p.partition): start[p] for p in assigned}},
                      endingOffsets={topic: {str(p.partition): end[p] for p in assigned}},
                      input=json.loads(Path(manifest).read_text()))
        if sum(end[p] - start[p] for p in assigned) != count:
            raise ValueError('Offset count differs: use a dedicated topic without concurrent producers')
        Path(output).write_text(json.dumps(result, indent=2))
        return result
    finally:
        producer.close()
        consumer.close()
