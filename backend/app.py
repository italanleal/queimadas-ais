"""Servidor opcional para desenvolvimento local; usa a mesma consulta da Lambda."""
import logging
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from backend.query import query_cells

app = FastAPI(title='Focos INPE • H3', version='3.0.0')
LOG = logging.getLogger(__name__)


@app.get('/api/health')
def health():
    return {'status': 'ok', 'time': datetime.now(timezone.utc).isoformat()}


REMOVED = ('day', 'window', 'view', 'minutes', 'as_of')


@app.get('/api/clusters')
def clusters(request: Request, satellite: str | None = Query(None, max_length=100),
             cursor: str | None = Query(None, max_length=4096),
             limit: int = Query(500, ge=1, le=1000)):
    removed = [name for name in REMOVED if name in request.query_params]
    if removed:
        raise HTTPException(400, f"Filtros de tempo não são mais suportados: {', '.join(removed)}")
    try:
        return query_cells(satellite, cursor, limit)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    except (ClientError, BotoCoreError):
        LOG.exception('Erro consultando DynamoDB')
        raise HTTPException(503, 'DynamoDB indisponível; confira região, tabela e credenciais')


app.mount('/', StaticFiles(directory=Path(__file__).resolve().parents[1] / 'frontend', html=True), name='frontend')
