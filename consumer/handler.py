"""Consumidor SQS FIFO: conta cada observação por célula H3, no agregado e por satélite.

O banco guarda apenas hash H3 + contagem; não há dimensão de tempo nem coordenadas.
"""
import json
import logging
import os

import boto3
import h3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from common.records import MODEL, cells_partition, record, satellites_partition

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
SERIALIZER = TypeSerializer()


def encode(item):
    return {k: SERIALIZER.serialize(v) for k, v in item.items()}


def increment(table, key):
    return {'Update': {'TableName': table, 'Key': encode(key),
                       'UpdateExpression': 'ADD #n :one',
                       'ExpressionAttributeNames': {'#n': 'count'},
                       'ExpressionAttributeValues': encode({':one': 1})}}


def persist(client, table, item, resolution):
    canonical = record(item['date'], item['satellite'], item['latitude'], item['longitude'])
    if item.get('id') != canonical['id']:
        raise ValueError('Identificador do registro não corresponde ao conteúdo')
    cell = h3.latlng_to_cell(canonical['latitude'], canonical['longitude'], resolution)
    satellite = canonical['satellite']
    marker = {'pk': f"EVENT#{canonical['id']}", 'sk': f'RES#{resolution}#{MODEL}'}
    try:
        # Marcador e contadores na mesma transação: reentregas não contam duas vezes.
        client.transact_write_items(TransactItems=[
            {'Put': {'TableName': table, 'Item': encode(marker), 'ConditionExpression': 'attribute_not_exists(pk)'}},
            increment(table, {'pk': cells_partition(resolution), 'sk': f'H3#{cell}'}),
            increment(table, {'pk': cells_partition(resolution, satellite), 'sk': f'H3#{cell}'}),
            increment(table, {'pk': satellites_partition(resolution), 'sk': f'SAT#{satellite}'}),
        ])
        return True
    except ClientError as error:
        if error.response['Error']['Code'] == 'TransactionCanceledException':
            reasons = error.response.get('CancellationReasons', [])
            if reasons and reasons[0].get('Code') == 'ConditionalCheckFailed':
                return False  # registro já contado
        raise


def handler(event, context):
    client = boto3.client('dynamodb')
    table = os.environ['TABLE_NAME']
    resolution = int(os.getenv('H3_RESOLUTION', '5'))
    if not 0 <= resolution <= 15:
        raise ValueError('Resolução H3 deve estar entre 0 e 15')
    messages = event.get('Records', [])
    for index, message in enumerate(messages):
        try:
            payload = json.loads(message['body'])
            records = payload['records']
            if payload.get('schema_version') != 1 or not isinstance(records, list) or not 1 <= len(records) <= 10:
                raise ValueError('Mensagem deve conter schema_version=1 e de 1 a 10 registros')
            for item in records:
                persist(client, table, item, resolution)
        except Exception:
            LOG.exception('Falha na mensagem %s', message['messageId'])
            # FIFO: interromper e devolver também as mensagens seguintes, preservando ordem.
            return {'batchItemFailures': [{'itemIdentifier': m['messageId']} for m in messages[index:]]}
    return {'batchItemFailures': []}
