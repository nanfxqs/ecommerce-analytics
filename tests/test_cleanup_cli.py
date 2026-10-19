"""Manual cleanup through its public command; remote effects injected at SSH boundary."""
import json
from pathlib import Path
import pytest
from retailrocket.cleanup import main

RUN = 'v2-'+'a'*32


class Remote:
    def __init__(self): self.calls=[]
    def cleanup(self, config, run_id, protected, apply):
        self.calls.append((run_id,protected,apply))
        return dict(run_id=run_id, applied=apply, paths=['edge/run','hdfs/run'])


def fixture(tmp_path, phase='completed'):
    root=tmp_path/'runs'; owned=root/RUN; owned.mkdir(parents=True)
    (owned/'run.json').write_text(json.dumps(dict(run_id=RUN,phase=phase,complete=True)))
    (root/'current.json').write_text((owned/'run.json').read_text())
    store=tmp_path/'published'; store.mkdir()
    config=tmp_path/'cluster.json'; config.write_text('{}')
    return root,store,config


def command(root,store,config,*extra):
    return ['--run-store',str(root),'--store',str(store),'--cluster-config',str(config),'--run-id',RUN,*extra]


def test_dry_run_retains_then_explicit_apply_removes_only_known_inactive_run(tmp_path,capsys):
    root,store,config=fixture(tmp_path); remote=Remote()
    neighbor=root/('v2-'+'b'*32); neighbor.mkdir()
    main(command(root,store,config),remote=remote)
    assert (root/RUN/'run.json').exists() and neighbor.exists()
    assert json.loads(capsys.readouterr().out)['applied'] is False
    main(command(root,store,config,'--apply'),remote=remote)
    assert not (root/RUN).exists() and neighbor.exists()
    assert not (root/'current.json').exists()
    assert remote.calls == [(RUN,None,False),(RUN,None,True)]


@pytest.mark.parametrize('phase',['running','paused','starting','draining','recovery_required','mystery'])
def test_active_or_uncertain_local_runs_are_retained(tmp_path,phase):
    root,store,config=fixture(tmp_path,phase); remote=Remote()
    with pytest.raises(ValueError,match='Active|unknown|uncertain'):
        main(command(root,store,config,'--apply'),remote=remote)
    assert (root/RUN).exists() and not remote.calls


def test_cli_rejects_unknown_identity_symlinks_live_supervisor_and_corrupt_publication(tmp_path):
    import fcntl
    root,store,config=fixture(tmp_path); remote=Remote()
    with pytest.raises(ValueError):
        main([*command(root,store,config)[:-1],'../escape'],remote=remote)
    (root/RUN/'redirect').symlink_to(tmp_path)
    with pytest.raises(ValueError,match='Symlink'):
        main(command(root,store,config,'--apply'),remote=remote)
    (root/RUN/'redirect').unlink()
    with (root/'supervisor.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        with pytest.raises(ValueError,match='supervisor'):
            main(command(root,store,config,'--apply'),remote=remote)
    (store/'current.json').write_text('{')
    with pytest.raises(ValueError):
        main(command(root,store,config,'--apply'),remote=remote)
    assert (root/RUN).exists() and not remote.calls


def test_current_validated_published_batch_is_protected(tmp_path,monkeypatch):
    from test_publication_cli import complete_export,publish
    root,store,config=fixture(tmp_path); remote=Remote()
    replay,export=complete_export(tmp_path,RUN)
    publish(monkeypatch,store,replay,export)
    with pytest.raises(ValueError,match='Current published'):
        main(command(root,store,config,'--apply'),remote=remote)
    assert (root/RUN).exists() and (store/'batches'/RUN).exists() and not remote.calls


def test_edge_command_refuses_live_worker_then_cleans_only_canonical_owned_paths(tmp_path,monkeypatch,capsys):
    import io,sys
    root=tmp_path/'edge'; owned=root/RUN; owned.mkdir(parents=True)
    state=dict(run_id=RUN,phase='completed',stream_released=True,output='hdfs://mycluster/retailrocket/streams/'+RUN,checkpoint='hdfs://mycluster/retailrocket/stream-checkpoints/'+RUN)
    (owned/'state.json').write_text(json.dumps(state)); (owned/'request.json').write_text(json.dumps(dict(run_id=RUN)))
    (owned/'launcher.json').write_text(json.dumps(dict(pid=99999999)))
    log=tmp_path/'commands.jsonl'
    external=tmp_path/'external.py'
    external.write_text('import json,sys\nfrom pathlib import Path\nargs=sys.argv[1:]\nwith Path('+repr(str(log))+').open("a") as f: f.write(json.dumps(args)+"\\n")\nif args[0]=="yarn": print("Total number of applications:0")\nelif args[1:4]==["dfs","-test","-e"]: sys.exit(0)\nelif "-ls" in args: print("drwxr-xr-x user group 0 2026-01-01 00:00 /safe")\n')
    config=tmp_path/'edge.json'; config.write_text(json.dumps(dict(root=str(root),hdfs_command=[sys.executable,str(external),'hdfs'],yarn_command=[sys.executable,str(external),'yarn'])))
    monkeypatch.setattr(sys,'stdin',io.StringIO(json.dumps(dict(protected_batch=None))))
    main(['--edge-config',str(config),'--run-id',RUN,'--apply'])
    assert not owned.exists()
    commands=[json.loads(line) for line in log.read_text().splitlines()]
    deleted=[args[-1] for args in commands if '-rm' in args]
    assert deleted == ['hdfs://mycluster/retailrocket/streams/'+RUN,'hdfs://mycluster/retailrocket/stream-checkpoints/'+RUN,'hdfs://mycluster/retailrocket/batches/'+RUN]


def test_previous_published_batch_cleans_without_changing_current_and_remote_failure_retains(tmp_path,monkeypatch):
    from test_publication_cli import complete_export,publish
    from retailrocket.publication import current_snapshot
    root,store,config=fixture(tmp_path)
    replay,export=complete_export(tmp_path,RUN); publish(monkeypatch,store,replay,export)
    replay,newer=complete_export(tmp_path,'newer'); publish(monkeypatch,store,replay,newer)
    class Failed:
        def cleanup(self,*args): raise RuntimeError('edge unreachable')
    with pytest.raises(RuntimeError,match='unreachable'):
        main(command(root,store,config,'--apply'),remote=Failed())
    assert (root/RUN).exists() and (store/'batches'/RUN).exists()
    main(command(root,store,config,'--apply'),remote=Remote())
    assert not (root/RUN).exists() and not (store/'batches'/RUN).exists()
    assert current_snapshot(store).batch_id == 'newer'


@pytest.mark.parametrize('case',['pid','lock','yarn','unsafe_path','protected','symlink'])
def test_edge_command_fails_closed_before_deletion_for_uncertain_ownership(tmp_path,monkeypatch,case):
    import io,os,sys,fcntl
    root=tmp_path/'edge'; owned=root/RUN; owned.mkdir(parents=True)
    state=dict(run_id=RUN,phase='completed',stream_released=True,output='hdfs://mycluster/retailrocket/streams/'+RUN,checkpoint='hdfs://mycluster/retailrocket/stream-checkpoints/'+RUN)
    if case=='unsafe_path': state['output']='hdfs://mycluster/retailrocket/streams/../other'
    (owned/'state.json').write_text(json.dumps(state)); (owned/'request.json').write_text(json.dumps(dict(run_id=RUN)))
    (owned/'launcher.json').write_text(json.dumps(dict(pid=os.getpid() if case=='pid' else 99999999)))
    if case=='symlink': (owned/'escape').symlink_to(tmp_path)
    tool=tmp_path/'tool.py'; tool.write_text('print("Total number of applications:1")')
    config=tmp_path/'edge.json'; config.write_text(json.dumps(dict(root=str(root),yarn_command=[sys.executable,str(tool)])))
    lock=(owned/'worker.lock').open('a')
    try:
        if case=='lock': fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        monkeypatch.setattr(sys,'stdin',io.StringIO(json.dumps(dict(protected_batch=RUN if case=='protected' else None))))
        with pytest.raises(ValueError): main(['--edge-config',str(config),'--run-id',RUN,'--apply'])
        assert (owned/'state.json').exists()
    finally: lock.close()


def test_unknown_or_missing_publication_state_and_ancestry_alias_are_never_deleted(tmp_path):
    root,store,config=fixture(tmp_path); remote=Remote()
    old=store/'batches'/'old'; old.mkdir(parents=True)
    with pytest.raises(ValueError,match='Missing publication pointer'):
        main(command(root,store,config,'--apply'),remote=remote)
    alias=tmp_path/'alias'; alias.symlink_to(root,target_is_directory=True)
    with pytest.raises(ValueError,match='Symlink'):
        main(command(alias,store,config,'--apply'),remote=remote)
    assert (root/RUN).exists() and old.exists() and not remote.calls
