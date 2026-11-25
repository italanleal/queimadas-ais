"""Contrato HTTP da Function URL; consulta substituída por um stub explícito."""
import importlib.util
import json
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest


@pytest.fixture
def api(monkeypatch):
    service = ModuleType('backend.query')
    service.query_cells = Mock(return_value={'type': 'FeatureCollection', 'features': [], 'next_cursor': None, 'satellites': []})
    errors = ModuleType('botocore.exceptions')
    errors.BotoCoreError = type('BotoCoreError', (Exception,), {})
    errors.ClientError = type('ClientError', (Exception,), {})
    monkeypatch.setitem(sys.modules, 'backend.query', service)
    monkeypatch.setitem(sys.modules, 'botocore.exceptions', errors)
    spec = importlib.util.spec_from_file_location('public_api_under_test', 'backend/handler.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, service.query_cells, errors


def event(path='/api/clusters', method='GET', params=None):
    return {'version': '2.0', 'rawPath': path, 'requestContext': {'http': {'method': method}},
            'queryStringParameters': params}


def test_function_url_success_and_pagination_forwarded(api):
    module, query, _ = api
    result = module.handler(event(params={'satellite': 'AQUA', 'cursor': 'abc', 'limit': '10'}), None)
    assert result['statusCode'] == 200
    assert json.loads(result['body'])['type'] == 'FeatureCollection'
    query.assert_called_once_with(satellite='AQUA', cursor='abc', limit=10)
    assert result['headers']['Cache-Control'] == 'no-store'
    assert not any(h.lower().startswith('access-control') for h in result['headers'])


def test_aggregate_is_default(api):
    module, query, _ = api
    assert module.handler(event(), None)['statusCode'] == 200
    query.assert_called_once_with(satellite=None, cursor=None, limit=500)


@pytest.mark.parametrize('path', ['/', '/api/health'])
def test_health_does_not_query_database(api, path):
    module, query, _ = api
    assert module.handler(event(path), None)['statusCode'] == 200
    query.assert_not_called()


def test_methods_and_routes(api):
    module, query, _ = api
    assert module.handler(event(method='POST'), None)['statusCode'] == 405
    assert module.handler(event('/unknown'), None)['statusCode'] == 404
    query.assert_not_called()


@pytest.mark.parametrize('params', [
    {'day': '2026-11-13'}, {'window': '15:40'}, {'view': 'recent'}, {'minutes': '60'},
    {'as_of': '2026-11-13T15:00:00Z'}, {'satellite': 'AQUA', 'day': '2026-11-13'},
    {'limit': 'bad'}, {'unknown': '1'},
])
def test_removed_time_filters_and_bad_params_return_400(api, params):
    module, query, _ = api
    assert module.handler(event(params=params), None)['statusCode'] == 400
    query.assert_not_called()


def test_query_validation_returns_400(api):
    module, query, _ = api
    query.side_effect = ValueError('Cursor inválido')
    assert module.handler(event(params={'cursor': 'bad'}), None)['statusCode'] == 400


def test_errors_do_not_expose_internal_details(api):
    module, query, errors = api
    query.side_effect = errors.ClientError('secret configuration')
    result = module.handler(event(), None)
    assert result['statusCode'] == 503
    assert 'secret' not in result['body']
    query.side_effect = RuntimeError('internal path')
    result = module.handler(event(), None)
    assert result['statusCode'] == 500
    assert 'internal path' not in result['body']
