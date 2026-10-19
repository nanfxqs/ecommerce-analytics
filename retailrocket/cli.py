import argparse
from retailrocket.ingest import prepare, replay


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    source = commands.add_parser('prepare')
    source.add_argument('--source', required=True)
    source.add_argument('--output', required=True)
    source.add_argument('--version', required=True)
    source.add_argument('--limit', type=int)
    send = commands.add_parser('replay')
    send.add_argument('--manifest', required=True)
    send.add_argument('--brokers', required=True)
    send.add_argument('--topic', required=True)
    send.add_argument('--batch-id', required=True)
    send.add_argument('--output', required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        print(prepare(args.source, args.output, args.version, args.limit))
    else:
        replay(args.manifest, args.brokers, args.topic, args.batch_id, args.output)


if __name__ == '__main__':
    main()
