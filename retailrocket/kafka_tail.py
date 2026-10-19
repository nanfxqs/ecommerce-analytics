"""Small, independent Kafka tail sample; never changes business/checkpoint progress."""
import json
import time


def sample_tail(consumer,topic,ends,limit=20,budget_ms=150):
    from kafka import TopicPartition
    partitions=[TopicPartition(topic,int(p)) for p,end in sorted(ends.items()) if end>0]
    if not partitions:return [],True
    consumer.assign(partitions)
    for partition in partitions:consumer.seek(partition,max(0,ends[str(partition.partition)]-limit))
    deadline=time.monotonic()+budget_ms/1000
    records={};last={}
    while time.monotonic()<deadline:
        remaining=max(1,int((deadline-time.monotonic())*1000))
        fetched=consumer.poll(timeout_ms=min(budget_ms,remaining),max_records=limit*len(partitions))
        if not fetched:break
        for partition,rows in fetched.items():
            if partition not in partitions:raise ValueError('Unexpected Kafka tail partition')
            for row in rows:
                if row.topic!=topic or row.partition!=partition.partition:raise ValueError('Kafka sample identity mismatch')
                if row.offset>=ends[str(partition.partition)]:continue
                value=row.value.decode('utf-8',errors='replace') if row.value is not None else None
                try:record=json.loads(value) if value is not None else None
                except (ValueError,TypeError):record=None
                records[(row.partition,row.offset)]=dict(topic=row.topic,partition=row.partition,offset=row.offset,
                    kafka_timestamp_ms=row.timestamp,value=value,record=record)
                last[row.partition]=max(last.get(row.partition,-1),row.offset)
        if all(last.get(p.partition,-1)>=ends[str(p.partition)]-1 for p in partitions):break
    complete=all(last.get(p.partition,-1)>=ends[str(p.partition)]-1 for p in partitions)
    newest=sorted(records.values(),key=lambda row:(row['kafka_timestamp_ms'] or -1,row['offset'],row['partition']),reverse=True)
    return newest[:limit],complete
