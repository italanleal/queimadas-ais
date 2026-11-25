"""Consulta das contagens H3, compartilhada pela Lambda pública e pelo servidor local."""
import base64
import json
import os
from datetime import datetime, timezone

import boto3
import h3
from boto3.dynamodb.conditions import Key

from common.records import cells_partition, satellites_partition


def _resolution():
    return int(os.getenv('H3_RESOLUTION', '5'))


def _satellites(table, resolution):
    """Catálogo de satélites com a contagem total de cada um."""
    request = {'KeyConditionExpression': Key('pk').eq(satellites_partition(resolution)), 'ConsistentRead': True}
    satellites = []
    while True:
        result = table.query(**request)
        for item in result.get('Items', []):
            satellites.append({'satellite': item['sk'].removeprefix('SAT#'), 'count': int(item['count'])})
        if not result.get('LastEvaluatedKey'):
            return satellites
        request['ExclusiveStartKey'] = result['LastEvaluatedKey']


def query_cells(satellite=None, cursor=None, limit=500):
    """Uma página de células H3 do agregado (satellite=None) ou de um satélite."""
    if satellite is not None:
        if not isinstance(satellite, str) or not satellite.strip() or len(satellite) > 100:
            raise ValueError('Satélite inválido')
        satellite = satellite.strip().upper()
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError('limit deve estar entre 1 e 1000')
    resolution = _resolution()
    partition = cells_partition(resolution, satellite)
    request = {'KeyConditionExpression': Key('pk').eq(partition), 'Limit': limit, 'ConsistentRead': True}
    if cursor is not None:
        try:
            if not isinstance(cursor, str) or len(cursor) > 4096:
                raise ValueError()
            start = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
            # O cursor só vale para a mesma partição (agregado ou satélite) em que foi emitido.
            if (not isinstance(start, dict) or set(start) != {'pk', 'sk'} or start['pk'] != partition
                    or not isinstance(start['sk'], str) or not start['sk'].startswith('H3#')):
                raise ValueError()
            request['ExclusiveStartKey'] = start
        except Exception:
            raise ValueError('Cursor inválido para esta consulta') from None
    table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])
    result = table.query(**request)
    features = []
    for item in result.get('Items', []):
        cell = item['sk'].removeprefix('H3#')
        ring = [[lng, lat] for lat, lng in h3.cell_to_boundary(cell)]
        ring.append(ring[0])
        features.append({'type': 'Feature', 'geometry': {'type': 'Polygon', 'coordinates': [ring]},
                         'properties': {'h3': cell, 'count': int(item['count']), 'resolution': resolution}})
    last = result.get('LastEvaluatedKey')
    body = {'type': 'FeatureCollection', 'features': features, 'satellite': satellite,
            'next_cursor': base64.urlsafe_b64encode(json.dumps(last).encode()).decode() if last else None,
            'queried_at': datetime.now(timezone.utc).isoformat()}
    if cursor is None:
        body['satellites'] = _satellites(table, resolution)
    return body
