from types import SimpleNamespace

import pytest

from retailrocket import demo, publication


def test_full_history_is_separate_and_disables_cluster(monkeypatch):
    monkeypatch.setattr(publication, 'current_snapshot', lambda _: SimpleNamespace(metadata={'input': {'rows': 2756101}}))
    original = {'store': '.demo/published', 'seed_store': '/tmp/full-source', 'cluster_config': 'cluster.json'}
    result = demo.full_history_config(original)
    assert result['store'] == '.demo/full-history-published'
    assert result['cluster_config'] is None
    assert original['cluster_config'] == 'cluster.json'
    assert original['store'] == '.demo/published'


def test_small_snapshot_is_not_accepted_as_full(monkeypatch):
    monkeypatch.setattr(publication, 'current_snapshot', lambda _: SimpleNamespace(metadata={'input': {'rows': 10000}}))
    with pytest.raises(RuntimeError, match='2,756,101'):
        demo.require_full_history('/tmp/small')


@pytest.mark.parametrize('target', ['/tmp/source', '.demo/published'])
def test_full_history_rejects_shared_store(target):
    with pytest.raises(RuntimeError, match='不能'):
        demo.full_history_config({'store': '.demo/published', 'seed_store': '/tmp/source', 'full_history_store': target})
