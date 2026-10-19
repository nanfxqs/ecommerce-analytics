"""CLI controls use the same durable HTTP boundary as the browser."""
import argparse
import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen


def main():
    parser=argparse.ArgumentParser(description='控制当前历史数据回放轮次')
    parser.add_argument('action',choices=['pause','continue','stop'])
    parser.add_argument('--url',default='http://127.0.0.1:8000')
    args=parser.parse_args()
    request=Request(args.url.rstrip('/')+'/api/replay/current/'+args.action,data=b'',method='POST')
    try:
        with urlopen(request,timeout=45) as response:
            print(json.dumps(json.load(response),ensure_ascii=False))
    except HTTPError as error:
        parser.exit(1,error.read().decode()+'\n')


if __name__=='__main__':main()
