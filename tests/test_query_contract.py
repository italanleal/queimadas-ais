"""Validação e paginação da consulta com SDK/geometria substituídos por doubles."""
import base64
import importlib.util
import json
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest


@pytest.fixture
def query_module(monkeypatch):
    class Key:
        def __init__(self, name): self.name = name
        def eq(self, value): self.value = value; return self
    sdk = ModuleType('boto3')
    table = Mock()
    sdk.resource = Mock()
    sdk.resource.return_value.Table.return_value = table
    conditions = ModuleType('boto3.dynamodb.conditions')
    conditions.Key = Key
    geometry = ModuleType('h3')
    geometry.cell_to_boundary = Mock(return_value=[(-10, -50), (-11, -50), (-10, -51)])
    monkeypatch.setitem(sys.modules, 'boto3', sdk)
    monkeypatch.setitem(sys.modules, 'boto3.dynamodb.conditions', conditions)
    monkeypatch.setitem(sys.modules, 'h3', geometry)
    monkeypatch.setenv('TABLE_NAME', 'clusters')
    monkeypatch.setenv('H3_RESOLUTION', '5')
    spec = importlib.util.spec_from_file_location('query_under_test', 'backend/query.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, table


def pages(cells, satellites=(), last=None):
    """Primeira chamada: células; segunda: catálogo de satélites."""
    return [{'Items': cells, **({'LastEvaluatedKey': last} if last else {})}, {'Items': list(satellites)}]


@pytest.mark.parametrize('params', [
    {'limit': 1001}, {'limit': 0}, {'limit': True}, {'limit': '10'},
    {'satellite': ''}, {'satellite': 'X' * 101}, {'satellite': 5},
    {'cursor': 'bad'}, {'cursor': 'x' * 5000},
])
def test_validation_before_database_call(query_module, params):
    module, table = query_module
    with pytest.raises(ValueError): module.query_cells(**params)
    table.query.assert_not_called()


def test_aggregate_geojson_satellites_and_cursor(query_module):
    module, table = query_module
    last = {'pk': 'RES#5#H3COUNT#ALL', 'sk': 'H3#abc'}
    table.query.side_effect = pages([{'pk': 'RES#5#H3COUNT#ALL', 'sk': 'H3#abc', 'count': 3}],
                                    [{'sk': 'SAT#AQUA', 'count': 2}, {'sk': 'SAT#TERRA', 'count': 1}], last)
    result = module.query_cells()
    cells_request = table.query.call_args_list[0].kwargs
    assert cells_request['KeyConditionExpression'].value == 'RES#5#H3COUNT#ALL'
    assert cells_request['ConsistentRead'] is True
    assert table.query.call_args_list[1].kwargs['KeyConditionExpression'].value == 'RES#5#H3COUNT#SATELLITES'
    feature = result['features'][0]
    assert feature['properties'] == {'h3': 'abc', 'count': 3, 'resolution': 5}
    ring = feature['geometry']['coordinates'][0]
    assert ring[0] == [-50, -10] and ring[-1] == ring[0]
    assert result['satellites'] == [{'satellite': 'AQUA', 'count': 2}, {'satellite': 'TERRA', 'count': 1}]
    assert json.loads(base64.urlsafe_b64decode(result['next_cursor'])) == last
    # Páginas seguintes não repetem o catálogo de satélites.
    table.query.reset_mock(side_effect=True)
    table.query.return_value = {'Items': []}
    second = module.query_cells(cursor=result['next_cursor'])
    assert table.query.call_count == 1
    assert table.query.call_args.kwargs['ExclusiveStartKey'] == last
    assert 'satellites' not in second and second['next_cursor'] is None


def test_satellite_partition_and_cursor_bound_to_partition(query_module):
    module, table = query_module
    last = {'pk': 'RES#5#H3COUNT#SAT#AQUA', 'sk': 'H3#abc'}
    table.query.side_effect = pages([{'sk': 'H3#abc', 'count': 2}], [{'sk': 'SAT#AQUA', 'count': 2}], last)
    result = module.query_cells(satellite=' aqua ')
    assert table.query.call_args_list[0].kwargs['KeyConditionExpression'].value == 'RES#5#H3COUNT#SAT#AQUA'
    assert result['satellite'] == 'AQUA'
    table.query.reset_mock(side_effect=True)
    with pytest.raises(ValueError): module.query_cells(cursor=result['next_cursor'])
    with pytest.raises(ValueError): module.query_cells(satellite='TERRA', cursor=result['next_cursor'])
    forged = base64.urlsafe_b64encode(json.dumps({'pk': 'RES#5#H3COUNT#SAT#AQUA', 'sk': 'SAT#AQUA'}).encode()).decode()
    with pytest.raises(ValueError): module.query_cells(satellite='AQUA', cursor=forged)
    table.query.assert_not_called()
