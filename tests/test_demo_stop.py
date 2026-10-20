import json
from unittest.mock import Mock

import pytest

from retailrocket import demo_stop


def test_drain_requests_stop_and_waits_for_release(monkeypatch):
    request = Mock(side_effect=[{'phase': 'running'}, {}, {'phase': 'draining'},
                               {'phase': 'stopped', 'lag': 0, 'stream_released': True}])
    monkeypatch.setattr(demo_stop, 'request', request)
    monkeypatch.setattr(demo_stop.time, 'sleep', lambda _: None)
    demo_stop.drain('http://test')
    assert request.call_args_list[1].args == ('http://test', 'stop')
    assert request.call_count == 4


@pytest.mark.parametrize('state', [{'phase': 'recovery_required'}, {'phase': 'failed'},
                                  {'phase': 'stopped', 'lag': 1, 'stream_released': True}])
def test_drain_refuses_unsafe_shutdown(monkeypatch, state):
    monkeypatch.setattr(demo_stop, 'request', lambda _: state)
    with pytest.raises(RuntimeError):
        demo_stop.drain('http://test')


def test_already_off_is_idempotent(tmp_path, monkeypatch):
    command = Mock(return_value='shut off')
    monkeypatch.setattr(demo_stop, 'command', command)
    demo_stop.stop(tmp_path, {'store': '.demo/published'})
    assert command.call_count == 3
    assert all(call.args[0][3] == 'domstate' for call in command.call_args_list)


def test_service_failure_prevents_vm_shutdown(tmp_path, monkeypatch):
    command = Mock(side_effect=['running'] * 3 + [RuntimeError('SSH unavailable')])
    monkeypatch.setattr(demo_stop, 'command', command)
    with pytest.raises(RuntimeError, match='SSH unavailable'):
        demo_stop.stop(tmp_path, {'store': '.demo/published'})
    assert not any('shutdown' in call.args[0] for call in command.call_args_list)


def test_unfinished_orphan_run_prevents_shutdown(tmp_path, monkeypatch):
    state = tmp_path / '.demo/replay-runs'
    state.mkdir(parents=True)
    (state / 'current.json').write_text(json.dumps({'phase': 'running'}))
    command = Mock()
    monkeypatch.setattr(demo_stop, 'command', command)
    with pytest.raises(RuntimeError, match='未结束轮次'):
        demo_stop.stop(tmp_path, {'store': '.demo/published'})
    command.assert_not_called()


def test_services_stop_before_graceful_vm_shutdown(tmp_path, monkeypatch):
    command = Mock(side_effect=['running'] * 3 + ['ok'] * 4 + ['shut off'] * 3)
    monkeypatch.setattr(demo_stop, 'command', command)
    demo_stop.stop(tmp_path, {'store': '.demo/published'})
    calls = [call.args[0] for call in command.call_args_list]
    assert calls[3][0] == 'bash'
    assert all(call[3] == 'shutdown' for call in calls[4:7])
    assert not any('destroy' in call for call in calls)


def test_active_launcher_receives_stop_request_and_releases_lease(tmp_path, monkeypatch):
    import fcntl
    import threading
    import time

    state = tmp_path / '.demo'
    state.mkdir()
    (state / 'runtime.json').write_text(json.dumps({'config': {'store': '.demo/published'}, 'cluster': True}))
    lease = (state / 'launcher.lock').open('a')
    fcntl.flock(lease, fcntl.LOCK_EX)
    drained = Mock()
    monkeypatch.setattr(demo_stop, 'drain', drained)
    command = Mock(return_value='shut off')
    monkeypatch.setattr(demo_stop, 'command', command)
    observed = []

    def launcher():
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if (state / 'stop.request').exists():
                observed.append(drained.call_count)
                lease.close()
                return
            time.sleep(.01)
        lease.close()

    thread = threading.Thread(target=launcher)
    thread.start()
    try:
        demo_stop.stop(tmp_path, {'store': 'ignored'})
    finally:
        thread.join()
    assert observed == [1]
    assert command.call_count == 3
