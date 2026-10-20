"""Graceful shutdown for the local demo and its explicitly configured VMs."""
import fcntl
import json
from pathlib import Path
import subprocess
import time
import urllib.request


def request(url, action=None):
    req = urllib.request.Request(url + ('/' + action if action else ''),
                                 data=b'' if action else None)
    with urllib.request.urlopen(req, timeout=150) as response:
        return json.load(response)


def drain(url, timeout=900):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = request(url)
        if state is None:
            return
        phase = state['phase']
        if phase in ('failed', 'recovery_required'):
            raise RuntimeError('轮次失败或待恢复，请先在页面处理；尚未关闭服务和虚拟机。')
        if phase in ('stopped', 'completed', 'published'):
            if not state.get('stream_released') or state.get('lag') != 0:
                raise RuntimeError('轮次尚未确认积压为 0 且流资源已释放。')
            return
        if phase in ('running', 'paused') and (state.get('pending_control') or {}).get('action') != 'stop':
            print('请求提前停止，等待排空……', flush=True)
            request(url, 'stop')
        time.sleep(1)
    raise RuntimeError('等待轮次结束超时；服务与虚拟机保持运行，请查看页面和日志。')


def command(args, timeout=180):
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or '命令失败')
    return result.stdout.strip()


def stop(root, config):
    state = root / '.demo'
    state.mkdir(exist_ok=True)
    with (state / 'shutdown.lock').open('a') as shutdown, (state / 'launcher.lock').open('a') as launcher:
        try:
            fcntl.flock(shutdown, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('已有停止操作正在执行。')
        active = False
        try:
            fcntl.flock(launcher, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            active = True
        if active:
            runtime = json.loads((state / 'runtime.json').read_text())
            config = runtime['config']
        if not active or not runtime['cluster']:
            store = Path(config['store']).expanduser()
            if not store.is_absolute():
                store = root / store
            current = store.parent / 'replay-runs/current.json'
            if current.exists():
                saved = json.loads(current.read_text())
                if (saved['phase'] not in ('stopped', 'completed', 'published')
                        or not saved.get('stream_released') or saved.get('lag') != 0):
                    raise RuntimeError('存在未结束轮次，请先运行 ./demo 并在页面恢复、停止后重试。')
        if active:
            if runtime['cluster']:
                drain(f"http://127.0.0.1:{config.get('api_port', 8000)}/api/replay/current")
            print('关闭演示网页和 API……', flush=True)
            (state / 'stop.request').touch()
            deadline = time.monotonic() + 180
            while True:
                try:
                    fcntl.flock(launcher, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() > deadline:
                        raise RuntimeError('演示入口未退出；尚未停止集群。请查看原启动终端。')
                    time.sleep(.5)
        vms = config.get('cluster_vms', ['vm-hadoop01', 'vm-hadoop02', 'vm-hadoop03'])
        states = {vm: command(['virsh', '-c', 'qemu:///system', 'domstate', vm]) for vm in vms}
        running = [vm for vm, status in states.items() if status != 'shut off']
        if running:
            if any(states[vm] != 'running' for vm in running):
                raise RuntimeError('虚拟机存在暂停等非运行状态，请先在虚拟机管理器处理。')
            script = Path(config.get('cluster_stop_script', '../hadoop-cluster/stop-cluster.sh')).expanduser()
            if not script.is_absolute():
                script = root / script
            print('按依赖顺序停止集群服务……', flush=True)
            command(['bash', str(script)], timeout=300)
            for vm in running:
                command(['virsh', '-c', 'qemu:///system', 'shutdown', vm])
            deadline = time.monotonic() + 180
            while running:
                running = [vm for vm in running if command(['virsh', '-c', 'qemu:///system', 'domstate', vm]) != 'shut off']
                if running and time.monotonic() > deadline:
                    raise RuntimeError('虚拟机正常关机超时：' + ', '.join(running) + '；未强制断电。')
                if running:
                    time.sleep(1)
        print('演示服务已停止，项目虚拟机均已关闭；数据与检查点保留。', flush=True)
