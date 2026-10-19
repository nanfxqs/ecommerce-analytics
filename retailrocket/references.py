"""Filter versioned category snapshots locally, then import directly into HDFS."""
import csv
import hashlib
import json
import shutil
import subprocess
from pathlib import Path


def prepare_references(properties, tree, output, version, events_manifest):
    events = json.loads(Path(events_manifest).read_text())
    if events['dataset_version'] != version:
        raise ValueError('Reference and event dataset versions differ')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    sources = []
    with (output/'categories.csv').open('w', newline='') as target:
        writer = csv.writer(target)
        writer.writerow(['timestamp','itemid','categoryid'])
        for source in properties:
            source = Path(source)
            rows = selected = 0
            checksum = hashlib.sha256()
            with source.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024*1024), b''):
                    checksum.update(chunk)
            with source.open(newline='') as stream:
                for row in csv.DictReader(stream):
                    rows += 1
                    if row['property'] == 'categoryid':
                        writer.writerow([row['timestamp'],row['itemid'],row['value']])
                        selected += 1
            sources.append(dict(file=source.name,sha256=checksum.hexdigest(),rows=rows,category_rows=selected))
    tree = Path(tree)
    shutil.copyfile(tree,output/'category_tree.csv')
    with tree.open(newline='') as stream:
        tree_rows=sum(1 for _ in csv.DictReader(stream))
    sources.append(dict(file=tree.name,sha256=hashlib.sha256(tree.read_bytes()).hexdigest(),rows=tree_rows))
    metadata = dict(dataset_version=version,events_input=events,properties='categories.csv',tree='category_tree.csv',sources=sources,loaded_properties=['categoryid'])
    (output/'references.json').write_text(json.dumps(metadata,indent=2))
    return output/'references.json'


def import_references(directory, hdfs_directory):
    """No Kafka transformation: copy registered references into a new HDFS path."""
    subprocess.run(['hdfs','dfs','-mkdir',hdfs_directory],check=True)
    for name in ('categories.csv','category_tree.csv','references.json'):
        subprocess.run(['hdfs','dfs','-put',str(Path(directory)/name),hdfs_directory+'/'+name],check=True)
