"""One foreground entry point for the prepared classroom demonstration."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


def local_path(value):
    path = Path(value).expanduser()
    return path if path.is_absolute() else ROOT / path


def require_full_history(store):
    from retailrocket.publication import current_snapshot
    snapshot = current_snapshot(store)
    if snapshot.metadata['input']['rows'] != 2756101:
        raise RuntimeError('全量展示需要已发布的 2,756,101 条完整历史；请检查 full_history_seed_store。')


def full_history_config(config):
    normal_store = local_path(config['store']).resolve()
    config = dict(config)
    config['seed_store'] = config.get('full_history_seed_store', config['seed_store'])
    config['store'] = config.get('full_history_store', '.demo/full-history-published')
    if local_path(config['store']).resolve() == local_path(config['seed_store']).resolve():
        raise RuntimeError('全量展示目录不能与源历史存储相同。')
    if local_path(config['store']).resolve() == normal_store:
        raise RuntimeError('全量展示目录不能与普通回放存储相同。')
    require_full_history(local_path(config['seed_store']))
    config['publication_store'] = str(normal_store)
    return config


def main():
    parser = argparse.ArgumentParser(description='统一启动课程演示，打开 http://127.0.0.1:5173；Ctrl+C 退出。')
    parser.add_argument('action', nargs='?', choices=['stop'], help='停止演示、集群并正常关闭项目虚拟机')
    parser.add_argument('--full-history', action='store_true', help='历史固定展示全量结果，同时启用真实集群回放控制')
    parser.add_argument('--history-only', action='store_true', help='仅展示已有历史结果，不启动或连接集群')
    parser.add_argument('--config', default='.demo/config.json', help='本机配置，详细说明见 docs/demo.md')
    args = parser.parse_args()
    os.chdir(ROOT)
    state = ROOT / '.demo'
    logs = state / 'logs'
    logs.mkdir(parents=True, exist_ok=True)
    if args.action == 'stop':
        from retailrocket.demo_stop import stop
        try:
            path = local_path(args.config)
            config = json.loads(path.read_text() if path.exists() else (ROOT / 'config/demo.example.json').read_text())
            stop(ROOT, config)
            return 0
        except (OSError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired) as error:
            print('停止失败：' + str(error), flush=True)
            return 1
    children = []
    handles = []

    def interrupt(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)

    def start(command, name, env=None):
        handle = (logs / (name + '.log')).open('a')
        handles.append(handle)
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=handle,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        children.append(process)
        return process

    def run(command, name, timeout):
        process = start(command, name)
        deadline = time.monotonic() + timeout
        while process.poll() is None:
            if time.monotonic() > deadline:
                raise RuntimeError(f'{name} 超时，查看 .demo/logs/{name}.log')
            time.sleep(.2)
        if process.returncode:
            raise RuntimeError(f'{name} 失败，查看 .demo/logs/{name}.log；离线展示可用 ./demo --history-only')

    def ready(url, process):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError('服务提前退出，请查看 .demo/logs/')
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, TimeoutError):
                pass
            time.sleep(.3)
        raise RuntimeError('服务未就绪，请查看 .demo/logs/')

    try:
        lease = (state / 'launcher.lock').open('a')
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('已有演示入口运行，请使用原终端，勿重复启动。')
        with (state / 'shutdown.lock').open('a') as shutdown:
            try:
                fcntl.flock(shutdown, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError('正在停止演示，请稍后再启动。')
        (state / 'stop.request').unlink(missing_ok=True)
        config_path = local_path(args.config)
        if not config_path.exists():
            if args.config != '.demo/config.json':
                raise RuntimeError('指定配置不存在：' + str(config_path))
            shutil.copyfile(ROOT / 'config/demo.example.json', config_path)
        config = json.loads(config_path.read_text())
        if args.full_history:
            config = full_history_config(config)
        api_port = int(config.get('api_port', 8000))
        web_port = int(config.get('web_port', 5173))
        if api_port == web_port or not all(1 <= p <= 65535 for p in (api_port, web_port)):
            raise RuntimeError('API 与网页必须使用不同的有效端口。')
        for port in (api_port, web_port):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(('127.0.0.1', port))
                except OSError:
                    raise RuntimeError(f'端口 {port} 已占用，请关闭原服务或修改 .demo/config.json；不会强杀其他进程。')
        if not (ROOT / 'frontend/node_modules/.bin/vite').exists():
            raise RuntimeError('缺少前端依赖，请运行 npm --prefix frontend ci；详见 docs/demo.md。')
        store = local_path(config['store'])
        if not (store / 'current.json').exists():
            from retailrocket.publication import current_snapshot
            seed = current_snapshot(local_path(config['seed_store']))
            if seed is None:
                raise RuntimeError('没有可用的历史快照，请先按 docs/demo.md 准备已发布数据。')
            target = store / 'batches' / seed.batch_id / seed.directory.name
            if target.exists():
                raise RuntimeError('发现尚未完成的演示副本，请按 docs/demo.md 检查，不自动覆盖。')
            print('首次准备：复制已验收的历史快照，保留原始发布存储。', flush=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            run(['cp', '-a', '--reflink=auto', str(seed.directory), str(target)], 'seed', 300)
            (store / 'current.json').write_text(json.dumps(dict(batch_id=seed.batch_id, revision=seed.directory.name)))
        if args.full_history:
            require_full_history(store)
        cluster = config.get('cluster_config') if not args.history_only else None
        if cluster:
            starter = config.get('cluster_start_script')
            if starter:
                print('启动既有三节点 Hadoop / Kafka（首次开机需要稍等）……', flush=True)
                run(['bash', str(local_path(starter))], 'cluster', 420)
                broker = ('export JAVA_HOME=/usr/local/jdk1.8.0_212; '
                          '"$JAVA_HOME/bin/jps" | grep -q Kafka || '
                          '~/opt/kafka_2.12-3.7.0/bin/kafka-server-start.sh -daemon '
                          '~/opt/kafka_2.12-3.7.0/config/server.properties')
                for host in ('h1', 'h2', 'h3'):
                    run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', host, broker], 'kafka-' + host, 30)
        (state / 'runtime.json').write_text(json.dumps(dict(config=config, cluster=bool(cluster))))
        api_url = f'http://127.0.0.1:{api_port}'
        command = [str(ROOT / '.venv/bin/python'), '-m', 'retailrocket.api', '--store', str(store),
                   '--port', str(api_port), '--poll-seconds', '0.25']
        if args.full_history:
            command += ['--publication-store', str(local_path(config['publication_store']))]
        if cluster:
            command += ['--cluster-config', str(local_path(cluster))]
        print('启动后台并校验历史结果……', flush=True)
        api = start(command, 'api')
        ready(api_url + '/api/history/overview', api)
        if cluster:
            ready(api_url + '/api/replay/datasets', api)
        print('构建并启动演示页面……', flush=True)
        run(['npm', '--prefix', 'frontend', 'run', 'build'], 'build', 180)
        env = dict(os.environ, RETAILROCKET_API=api_url)
        web = start(['npm', '--prefix', 'frontend', 'run', 'preview', '--', '--port', str(web_port), '--strictPort'], 'web', env)
        url = f'http://127.0.0.1:{web_port}'
        ready(url, web)
        print(f'\n演示已就绪：{url}\n模式：' + (('全量历史固定展示＋真实集群回放' if cluster else '全量历史展示（离线）') if args.full_history else ('历史分析＋真实集群回放' if cluster else '历史分析（离线）')) +
              '\n结束并关闭虚拟机：另开终端运行 ./demo stop；仅关闭页面/API 可按 Ctrl+C。' +
              '\n详细操作：docs/demo.md\n', flush=True)
        while api.poll() is None and web.poll() is None:
            if (state / 'stop.request').exists():
                raise KeyboardInterrupt
            time.sleep(.5)
        raise RuntimeError('演示服务已退出，请查看 .demo/logs/')
    except KeyboardInterrupt:
        print('\n正在关闭本次网页与 API；不删除数据，不关闭集群。', flush=True)
        return 0
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print('启动失败：' + str(error), flush=True)
        return 1
    finally:
        for process in reversed(children):
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                process.wait()
        for handle in handles:
            handle.close()
        if 'lease' in locals():
            lease.close()


if __name__ == '__main__':
    raise SystemExit(main())
