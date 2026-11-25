"""Integração AWS simulada: instalar requirements-dev.txt para executar."""
import json

import pytest
boto3 = pytest.importorskip('boto3')
h3 = pytest.importorskip('h3')
moto = pytest.importorskip('moto')
pytest.importorskip('fastapi')
pytest.importorskip('httpx')
from fastapi.testclient import TestClient
from backend.app import app
from common.records import record
from consumer.handler import handler


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'test')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'test')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('TABLE_NAME', 'test-clusters')
    monkeypatch.setenv('H3_RESOLUTION', '5')
    with moto.mock_aws():
        client = boto3.client('dynamodb')
        # Mesma chave do template: só pk/sk, sem índice secundário.
        client.create_table(TableName='test-clusters', BillingMode='PAY_PER_REQUEST',
                            KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}],
                            AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'}])
        yield client


def event(items):
    return {'Records': [{'messageId': 'm1', 'body': json.dumps({'schema_version': 1, 'records': items})}]}


def rows(client):
    return {(r['pk']['S'], r['sk']['S']): r for r in client.scan(TableName='test-clusters')['Items']}


def test_items_hold_only_hash_and_count(aws):
    item = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    assert handler(event([item]), None) == {'batchItemFailures': []}
    cell = h3.latlng_to_cell(-10, -50, 5)
    stored = rows(aws)
    assert set(stored) == {(f"EVENT#{item['id']}", 'RES#5#H3COUNT'),
                           ('RES#5#H3COUNT#ALL', f'H3#{cell}'),
                           ('RES#5#H3COUNT#SAT#AQUA', f'H3#{cell}'),
                           ('RES#5#H3COUNT#SATELLITES', 'SAT#AQUA')}
    for row in stored.values():
        assert not {'date', 'latitude', 'longitude'} & set(row)
        if not row['pk']['S'].startswith('EVENT#'):
            assert set(row) == {'pk', 'sk', 'count'} and row['count']['N'] == '1'


def test_duplicate_retry_does_not_increment(aws):
    item = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    assert handler(event([item]), None) == {'batchItemFailures': []}
    assert handler(event([item]), None) == {'batchItemFailures': []}
    stored = rows(aws)
    assert len(stored) == 4
    assert all(r['count']['N'] == '1' for r in stored.values() if 'count' in r)


def test_partial_message_retry_and_geojson_pagination(aws):
    first = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    second = record('2026-11-10T02:00:00Z', 'TERRA', -20, -45)
    bad = {**second, 'latitude': 999}
    assert handler(event([first, bad]), None)['batchItemFailures'] == [{'itemIdentifier': 'm1'}]
    assert handler(event([first, second]), None)['batchItemFailures'] == []
    with TestClient(app) as api:
        page = api.get('/api/clusters', params={'limit': 1}).json()
        assert page['next_cursor']
        assert page['satellites'] == [{'satellite': 'AQUA', 'count': 1}, {'satellite': 'TERRA', 'count': 1}]
        next_page = api.get('/api/clusters', params={'limit': 1, 'cursor': page['next_cursor']}).json()
        assert 'satellites' not in next_page
        features = page['features'] + next_page['features']
        assert sum(f['properties']['count'] for f in features) == 2
        ring = features[0]['geometry']['coordinates'][0]
        assert ring[0] == ring[-1]
        assert ring[0][0] < -40  # longitude antes de latitude
        assert api.get('/api/clusters?cursor=bad').status_code == 400
        # Cursor do agregado não vale na partição de um satélite.
        assert api.get('/api/clusters', params={'satellite': 'AQUA', 'cursor': page['next_cursor']}).status_code == 400
        assert api.get('/').status_code == 200


def test_fifo_stops_at_first_failure(aws):
    item = record('2026-11-10', 'AQUA', -10, -50)
    response = handler({'Records': [{'messageId': 'bad', 'body': '{}'}, event([item])['Records'][0]]}, None)
    assert response['batchItemFailures'] == [{'itemIdentifier': 'bad'}, {'itemIdentifier': 'm1'}]
    assert aws.scan(TableName='test-clusters')['Count'] == 0


def test_aggregate_and_satellite_filter_ignore_time(aws):
    items = [record('2000-01-01T23:35:00Z', 'AQUA', -10, -50),
             record('2026-11-10T15:41:00Z', 'AQUA', -10, -50),
             record('2026-11-10T15:42:00Z', 'TERRA', -10, -50)]
    assert handler(event(items), None)['batchItemFailures'] == []
    cell = h3.latlng_to_cell(-10, -50, 5)
    with TestClient(app) as api:
        total = api.get('/api/clusters').json()
        assert [(f['properties']['h3'], f['properties']['count']) for f in total['features']] == [(cell, 3)]
        aqua = api.get('/api/clusters?satellite=aqua').json()
        assert aqua['features'][0]['properties']['count'] == 2
        assert api.get('/api/clusters?satellite=GOES-19').json()['features'] == []
        for removed in ('day=2026-11-10', 'window=15:40', 'view=recent', 'minutes=10', 'as_of=2026-11-10T00:00:00Z'):
            assert api.get(f'/api/clusters?{removed}').status_code == 400


def test_function_url_with_real_query(aws):
    from backend.handler import handler as api_handler
    item = record('2026-11-10T15:41:00Z', 'AQUA', -10, -50)
    assert handler(event([item]), None)['batchItemFailures'] == []
    request = {'version': '2.0', 'rawPath': '/api/clusters', 'requestContext': {'http': {'method': 'GET'}},
               'queryStringParameters': {'satellite': 'AQUA'}}
    result = api_handler(request, None)
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['features'][0]['properties']['count'] == 1
    assert body['satellites'] == [{'satellite': 'AQUA', 'count': 1}]
    request['queryStringParameters'] = {'day': '2026-11-10'}
    assert api_handler(request, None)['statusCode'] == 400
