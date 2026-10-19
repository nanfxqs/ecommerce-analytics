"""Published known input -> HTTP history pages; independent literal expectations."""
import os
import pytest
from fastapi.testclient import TestClient
from retailrocket.api import create_app
from history_fixture import publish_known_history


@pytest.fixture(scope='module')
def client(tmp_path_factory):
    store = os.environ.get('HISTORY_TEST_STORE') or publish_known_history(tmp_path_factory.mktemp('history-pages'))
    with TestClient(create_app(store)) as client:
        yield client


def test_purchase_intersections_and_empty_denominators_use_shared_utc_range(client):
    response = client.get('/api/history/purchase?start=1970-01-01&end=1970-01-02')
    assert response.status_code == 200
    body = response.json()
    assert body['snapshot']['batch_id'] == 'known'
    assert body['selected_range'] == dict(start='1970-01-01', end='1970-01-02')
    data = body['data']
    assert (data['visitors'],data['view_visitors'],data['cart_visitors'],data['purchase_visitors']) == (3,1,1,2)
    assert (data['view_purchase_visitors'],data['cart_purchase_visitors']) == (1,0)
    assert (data['view_purchase_rate'],data['cart_purchase_rate']) == (1.,0.)
    empty = client.get('/api/history/purchase?start=1970-01-03&end=1970-01-03').json()['data']
    assert empty['visitors'] == 0 and empty['view_purchase_rate'] is None and empty['cart_purchase_rate'] is None


def test_categories_preserve_unknown_and_time_correct_association(client):
    response = client.get('/api/history/categories?start=1970-01-01&end=1970-01-02')
    assert response.status_code == 200
    data = response.json()['data']
    assert data['coverage'] == dict(total=6, associated=5, unknown=1, rate=5/6)
    assert data['categories'] == [dict(categoryid='10',events=5,views=2,addtocarts=1,transactions=2),dict(categoryid='unknown',events=1,views=1,addtocarts=0,transactions=0)]
    assert data['items'][0] == dict(itemid='a',events=5,views=2,addtocarts=1,transactions=2)
    empty = client.get('/api/history/categories?start=1970-01-03&end=1970-01-03').json()['data']
    assert empty == dict(items=[],categories=[],coverage=dict(total=0,associated=0,unknown=0,rate=None))


def test_profiles_have_exact_lookup_pagination_and_range_relative_recency(client):
    response = client.get('/api/history/profiles?start=1970-01-01&end=1970-01-02&page_size=1')
    assert response.status_code == 200
    data = response.json()['data']
    assert (data['total'],data['page'],data['page_size']) == (3,1,1)
    visitor = data['visitors'][0]
    assert (visitor['visitorid'],visitor['active_days'],visitor['events'],visitor['purchases'],visitor['recency_seconds']) == ('1',2,4,1,86399.998)
    assert visitor['categories'] == [dict(categoryid='10',views=2,purchases=1),dict(categoryid='unknown',views=1,purchases=0)]
    assert client.get('/api/history/profiles?start=1970-01-01&end=1970-01-02&page_number=2&page_size=1').json()['data']['visitors'][0]['visitorid'] == '2'
    assert client.get('/api/history/profiles?visitorid=1%20OR%201=1').json()['data']['total'] == 0
    assert client.get('/api/history/profiles?page_number=999').json()['data']['visitors'] == []
    assert client.get('/api/history/profiles?start=1970-01-03&end=1970-01-03').json()['data']['total'] == 0
    for params in ('page_number=0','page_size=101','top_n=0'):
        assert client.get('/api/history/profiles?'+params).status_code == 422


def test_quality_keeps_invalid_time_in_batch_scope_and_reconciles_duplicates(client):
    response = client.get('/api/history/quality?start=1970-01-01&end=1970-01-02')
    assert response.status_code == 200
    data = response.json()['data']
    assert {k:data['batch_quality'][k] for k in ('input_count','consumed_count','unique_source_count','valid_count','invalid_count','retry_count','suspected_duplicate_count')} == dict(input_count=8,consumed_count=9,unique_source_count=8,valid_count=7,invalid_count=1,retry_count=1,suspected_duplicate_count=1)
    assert data['range_quality'] == dict(valid_count=6,invalid_count=0)
    assert len(data['reasons']) == 1 and data['reasons'][0]['count'] == 1
    assert data['records'][0]['timestamp'] == 'bad'
    assert data['records'][0]['source_line'] == 7
    assert data['records'][0]['event_time'] is None
    scoped = client.get('/api/history/quality?start=1970-01-01&end=1970-01-02&scope=range').json()['data']
    assert scoped['anomaly_scope'] == 'range' and scoped['reasons'] == [] and scoped['records'] == []
    assert client.get('/api/history/quality?limit=501').status_code == 422
    assert client.get('/api/history/quality?scope=unknown').status_code == 422
