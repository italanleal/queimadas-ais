"""Endpoint HTTP público para Lambda Function URL (payload 2.0)."""
import json
import logging
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError
from backend.query import query_cells

LOG = logging.getLogger(__name__)
ALLOWED = {'satellite', 'cursor', 'limit'}
REMOVED = {'day', 'window', 'view', 'minutes', 'as_of'}


def response(status, body):
    # CORS pertence à configuração da Function URL, evitando cabeçalhos duplicados.
    return {'statusCode': status, 'headers': {'Content-Type': 'application/json; charset=utf-8',
                                            'Cache-Control': 'no-store'},
            'isBase64Encoded': False, 'body': json.dumps(body, ensure_ascii=False)}


def handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', '')
    if method != 'GET':
        result = response(405, {'detail': 'Método não permitido'})
        result['headers']['Allow'] = 'GET'
        return result
    path = event.get('rawPath', '/')
    if path in ('/', '/api/health'):
        return response(200, {'status': 'ok', 'time': datetime.now(timezone.utc).isoformat()})
    if path != '/api/clusters':
        return response(404, {'detail': 'Rota não encontrada'})
    params = event.get('queryStringParameters') or {}
    try:
        if not isinstance(params, dict):
            raise ValueError('Parâmetros inválidos')
        removed = sorted(REMOVED.intersection(params))
        if removed:
            raise ValueError(f"Filtros de tempo não são mais suportados: {', '.join(removed)}")
        unknown = sorted(set(params) - ALLOWED)
        if unknown:
            raise ValueError(f"Parâmetros desconhecidos: {', '.join(unknown)}")
        try:
            limit = int(params.get('limit', '500'))
        except (ValueError, TypeError):
            raise ValueError('limit deve ser um número inteiro') from None
        data = query_cells(satellite=params.get('satellite'), cursor=params.get('cursor'), limit=limit)
        return response(200, data)
    except ValueError as error:
        return response(400, {'detail': str(error)})
    except (BotoCoreError, ClientError):
        LOG.exception('Falha consultando DynamoDB')
        return response(503, {'detail': 'Consulta indisponível temporariamente'})
    except Exception:
        LOG.exception('Erro interno na consulta')
        return response(500, {'detail': 'Erro interno'})
