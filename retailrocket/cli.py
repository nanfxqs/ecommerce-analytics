import argparse
import json
from pathlib import Path
from retailrocket.ingest import prepare, replay
from retailrocket.publication import BatchRun, current_snapshot, publish_batch


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    refs = commands.add_parser('prepare-references')
    refs.add_argument('--properties', nargs='+', required=True)
    refs.add_argument('--tree', required=True)
    refs.add_argument('--output', required=True)
    refs.add_argument('--version', required=True)
    refs.add_argument('--events-manifest', required=True)
    refs.add_argument('--hdfs-directory')
    source = commands.add_parser('prepare')
    source.add_argument('--source', required=True)
    source.add_argument('--output', required=True)
    source.add_argument('--version', required=True)
    source.add_argument('--limit', type=int)
    source.add_argument('--store')
    source.add_argument('--batch-id')
    send = commands.add_parser('replay')
    send.add_argument('--manifest', required=True)
    send.add_argument('--brokers', required=True)
    send.add_argument('--topic', required=True)
    send.add_argument('--batch-id', required=True)
    send.add_argument('--output', required=True)
    send.add_argument('--store')
    publish = commands.add_parser('publish')
    publish.add_argument('--store', required=True)
    publish.add_argument('--replay', required=True)
    publish.add_argument('--export', dest='export_directory', required=True)
    status = commands.add_parser('status')
    status.add_argument('--store', required=True)
    status.add_argument('--batch-id')
    args = parser.parse_args()
    if args.command == 'prepare-references':
        from retailrocket.references import prepare_references, import_references
        manifest = prepare_references(args.properties,args.tree,args.output,args.version,args.events_manifest)
        if args.hdfs_directory:
            import_references(manifest.parent,args.hdfs_directory)
        print(manifest)
    elif args.command == 'publish':
        from retailrocket.validation import validate_analysis
        result = publish_batch(args.store, args.replay, args.export_directory, validate=validate_analysis)
        print(json.dumps(dict(batch_id=result.batch_id, directory=str(result.directory))))
    elif args.command == 'status':
        if args.batch_id:
            print(BatchRun(args.store, args.batch_id).path.read_text())
        else:
            result = current_snapshot(args.store)
            print(json.dumps(dict(batch_id=result.batch_id, directory=str(result.directory), metadata=result.metadata)))
    elif args.command == 'prepare':
        if args.store and not args.batch_id:
            parser.error('--store requires --batch-id for prepare')
        if args.store:
            run = BatchRun(args.store, args.batch_id)
            with run.tracking('preparing', source=str(Path(args.source).resolve()), dataset_version=args.version):
                result = prepare(args.source, args.output, args.version, args.limit)
                run.stage('prepared', records=json.loads(result.read_text())['rows'], input=json.loads(result.read_text()), manifest=str(result.resolve()))
        else:
            result = prepare(args.source, args.output, args.version, args.limit)
        print(result)
    else:
        store = args.store or str(Path(args.output).parent / 'batch-state')
        run = BatchRun(store, args.batch_id)
        with run.tracking('replaying', manifest=str(Path(args.manifest).resolve()), topic=args.topic):
            run.stage('replaying', input=json.loads(Path(args.manifest).read_text()), prepared_directory=str(Path(args.manifest).resolve().parent))
            result = replay(args.manifest, args.brokers, args.topic, args.batch_id, args.output)
            run.stage('replayed', records=result['rows'], replay=result)


if __name__ == '__main__':
    main()
