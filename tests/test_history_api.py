"""Approved HTTP seam: published known batch -> range-filtered overview."""
import pytest
from fastapi.testclient import TestClient
from retailrocket.api import create_app
from history_fixture import publish_known_history


@pytest.fixture(scope='module')
def history_store(tmp_path_factory):
    return publish_known_history(tmp_path_factory.mktemp('history'))


def test_overview_binds_one_published_revision_and_inclusive_utc_dates(history_store):
    with TestClient(create_app(history_store)) as client:
        response = client.get('/api/history/overview', params={'start':'1970-01-01','end':'1970-01-02'})
    assert response.status_code == 200
    body = response.json()
    assert body['snapshot']['batch_id'] == 'known'
    assert len(body['snapshot']['revision']) == 64
    assert body['snapshot']['complete'] is True
    assert body['snapshot']['timezone'] == 'UTC'
    assert body['snapshot']['event_bounds'] == {'start':'1970-01-01','end':'1970-01-04'}
    assert body['snapshot']['updated_at']
    assert body['selected_range'] == {'start':'1970-01-01','end':'1970-01-02'}
    assert body['data']['counts'] == {'view':3,'addtocart':1,'transaction':2}
    assert body['data']['visitors']['visitors'] == 3
    assert [(x['day'],x['visitors']) for x in body['data']['trend']] == [('1970-01-01',1),('1970-01-02',3)]


def test_empty_dates_invalid_ranges_and_unpublished_store(history_store, tmp_path):
    with TestClient(create_app(history_store)) as client:
        empty = client.get('/api/history/overview', params={'start':'1970-01-03','end':'1970-01-03'}).json()
        assert empty['data']['counts'] == dict(view=0,addtocart=0,transaction=0)
        assert empty['data']['visitors']['visitors'] == 0
        assert empty['data']['trend'] == []
        for params in ({'start':'1970-01-02'}, {'start':'bad','end':'1970-01-03'}, {'start':'1970-01-04','end':'1970-01-01'}):
            assert client.get('/api/history/overview', params=params).status_code == 422
    with TestClient(create_app(tmp_path/'empty-store')) as client:
        assert client.get('/api/history/overview').json() == dict(snapshot=None, selected_range=None, data=None)


def test_cached_history_does_not_hide_corruption_or_serve_removed_pointer(history_store, tmp_path):
    import shutil
    store = tmp_path/'store'
    shutil.copytree(history_store, store)
    with TestClient(create_app(store)) as client:
        before = client.get('/api/history/overview')
        assert before.status_code == 200
        metadata = next(store.glob('batches/*/*/metadata.json'))
        original = metadata.read_bytes()
        metadata.write_text('{}')
        assert client.get('/api/history/overview').status_code == 503
        metadata.write_bytes(original)
        assert client.get('/api/history/overview').json() == before.json()
        (store/'current.json').unlink()
        assert client.get('/api/history/overview').json()['snapshot'] is None
