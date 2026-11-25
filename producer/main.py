"""Rotina local: descobrir → baixar → consolidar/ordenar → S3 → stream SQS."""
import argparse
import csv
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import time
from html.parser import HTMLParser
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from common.records import from_csv, key
from producer.local_state import reset_local_data

SOURCE = 'https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/10min/'
LOG = logging.getLogger(__name__)


class Listing(HTMLParser):
    def __init__(self):
        super().__init__()
        self.files = set()

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get('href', '')
        if tag == 'a' and re.fullmatch(r'focos_10min_\d{8}_\d{4}\.csv', href):
            self.files.add(href)


def fetch(url, target=None):
    for attempt in range(4):
        try:
            with urlopen(Request(url, headers={'User-Agent': 'INPE-H3-Educational/1.0'}), timeout=90) as response:
                if target is None:
                    return response.read().decode('utf-8-sig')
                temporary = target.with_suffix('.part')
                with temporary.open('wb') as output:
                    while chunk := response.read(1024 * 1024):
                        output.write(chunk)
                temporary.replace(target)
                return
        except Exception:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)


def connect(path):
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, date TEXT NOT NULL, payload TEXT NOT NULL)')
    db.execute('CREATE INDEX IF NOT EXISTS ordered ON records(date,id)')
    db.execute('CREATE TABLE IF NOT EXISTS files (name TEXT PRIMARY KEY)')
    db.execute('CREATE TABLE IF NOT EXISTS sent (destination TEXT, id TEXT, PRIMARY KEY(destination,id))')
    db.commit()
    return db


def ingest(db, path, brazil_only=True):
    """Uma transação por arquivo; falhas não marcam o arquivo como concluído."""
    with path.open(encoding='utf-8-sig', newline='') as source:
        sample = source.read(8192)
        source.seek(0)
        # Arquivos vazios e avisos de ausência de focos não são datasets.
        if not sample.strip():
            return 0
        if '\n' not in sample.strip() and any(s in key(sample) for s in ('semdados', 'semfocos', 'nodata', 'norecords', 'nenhumfoco')):
            return 0
        delimiter = ';' if sample.splitlines()[0].count(';') > sample.splitlines()[0].count(',') else ','
        reader = csv.DictReader(source, delimiter=delimiter)
        columns = {key(c) for c in (reader.fieldnames or [])}
        required = [{'datahoragmt', 'datahora', 'data', 'datetime', 'timestamp'},
                    {'satelite', 'satellite'}, {'lat', 'latitude'}, {'lon', 'long', 'longitude'}]
        if any(not columns.intersection(group) for group in required):
            raise ValueError(f'{path.name}: cabeçalho CSV não reconhecido: {reader.fieldnames}')
        if brazil_only and not columns.intersection({'pais', 'country'}):
            LOG.warning('%s: sem coluna de país; serão preservadas todas as observações, inclusive fora do Brasil', path.name)
        added = 0
        with db:
            for line, row in enumerate(reader, 2):
                try:
                    item = from_csv(row, brazil_only)
                except (ValueError, TypeError) as error:
                    raise ValueError(f'{path.name}, linha {line}: {error}') from error
                if item:
                    result = db.execute('INSERT OR IGNORE INTO records VALUES (?,?,?)',
                                        (item['id'], item['date'], json.dumps(item, ensure_ascii=False)))
                    added += result.rowcount
        return added


def consolidate(db, directory):
    path = directory / 'consolidated.csv'
    temporary = directory / 'consolidated.tmp'
    fields = ['id', 'date', 'satellite', 'latitude', 'longitude']
    with temporary.open('w', encoding='utf-8', newline='') as output:
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        for (payload,) in db.execute('SELECT payload FROM records ORDER BY date,id'):
            writer.writerow(json.loads(payload))
    temporary.replace(path)
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    return path, digest


def batches(rows, size):
    if not 1 <= size <= 10:
        raise ValueError('RECORDS_PER_MESSAGE deve estar entre 1 e 10')
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def upload_dataset(s3, bucket, path, digest, directory):
    object_key = f'consolidated/{digest}.csv'
    s3.upload_file(str(path), bucket, object_key, ExtraArgs={'ContentType': 'text/csv; charset=utf-8'})
    manifest = {'bucket': bucket, 'key': object_key, 'sha256': digest}
    temporary = directory / 's3-dataset.tmp'
    temporary.write_text(json.dumps(manifest), encoding='utf-8')
    temporary.replace(directory / 's3-dataset.json')
    LOG.info('Dataset enviado: s3://%s/%s', bucket, object_key)
    return manifest


def stream_dataset(db, s3, sqs, bucket, queue_url, object_key, size, delay):
    if not 1 <= size <= 10 or delay < 0:
        raise ValueError('Lote deve ser 1..10 e intervalo deve ser >= 0')
    response = s3.get_object(Bucket=bucket, Key=object_key)
    count = 0
    # O stream é lido do S3; nunca carregamos o dataset inteiro na memória.
    with io.TextIOWrapper(response['Body'], encoding='utf-8', newline='') as stream:
        def pending():
            for row in csv.DictReader(stream):
                if db.execute('SELECT 1 FROM sent WHERE destination=? AND id=?', (queue_url, row['id'])).fetchone():
                    continue
                row['latitude'], row['longitude'] = float(row['latitude']), float(row['longitude'])
                yield row
        for items in batches(pending(), size):
            body = json.dumps({'schema_version': 1, 'records': items}, ensure_ascii=False, separators=(',', ':'))
            sqs.send_message(QueueUrl=queue_url, MessageBody=body, MessageGroupId='inpe-chronological',
                             MessageDeduplicationId=hashlib.sha256(body.encode()).hexdigest())
            with db:
                db.executemany('INSERT OR IGNORE INTO sent VALUES (?,?)', [(queue_url, r['id']) for r in items])
            count += len(items)
            LOG.info('Enviados %s registros, total desta rodada=%s', len(items), count)
            time.sleep(delay)  # variável para calibrar a velocidade da leitura/envio
    return count


def publish(db, s3, sqs, bucket, queue_url, path, digest, size, delay):
    """Compatibilidade para callers que executam upload e stream juntos."""
    if not 1 <= size <= 10 or delay < 0:
        raise ValueError('Lote deve ser 1..10 e intervalo deve ser >= 0')
    manifest = upload_dataset(s3, bucket, path, digest, path.parent)
    return stream_dataset(db, s3, sqs, bucket, queue_url, manifest['key'], size, delay)


def scrape_dataset(args, db):
    parser = Listing()
    parser.feed(fetch(SOURCE))
    names = sorted(parser.files)
    if not names:
        raise RuntimeError('A listagem do INPE não retornou arquivos CSV reconhecidos')
    if args.max_files:
        names = names[-args.max_files:]
    for name in names:
        if db.execute('SELECT 1 FROM files WHERE name=?', (name,)).fetchone():
            continue
        path = args.data_dir / 'raw' / name
        if not path.exists():
            fetch(urljoin(SOURCE, name), path)
        added = ingest(db, path, not args.all_countries)
        with db:
            db.execute('INSERT INTO files VALUES (?)', (name,))
        LOG.info('%s: %s novos registros', name, added)
    path, digest = consolidate(db, args.data_dir)
    LOG.info('Dataset ordenado: %s (sha256=%s)', path, digest)
    return path, digest


def prepared_dataset(args):
    path = args.data_dir / 's3-dataset.json'
    if not path.is_file():
        raise ValueError('Nenhum dataset enviado ao S3. Execute --scrape primeiro.')
    manifest = json.loads(path.read_text(encoding='utf-8'))
    digest = manifest.get('sha256', '')
    if (manifest.get('bucket') != args.bucket or not re.fullmatch(r'[0-9a-f]{64}', digest)
            or manifest.get('key') != f'consolidated/{digest}.csv'):
        raise ValueError('Manifesto S3 inválido ou bucket diferente. Execute --scrape para este bucket.')
    return manifest


def routine(args, db, s3=None, sqs=None):
    if args.scrape:
        path, digest = scrape_dataset(args, db)
        if not args.prepare_only:
            upload_dataset(s3, args.bucket, path, digest, args.data_dir)
    if args.start:
        manifest = prepared_dataset(args)
        return stream_dataset(db, s3, sqs, args.bucket, args.queue_url, manifest['key'],
                              args.records_per_message, args.read_interval_seconds)


def aws_clients(start=False):
    import boto3
    from botocore.config import Config
    config = Config(retries={'mode': 'standard', 'max_attempts': 8})
    return boto3.client('s3', config=config), boto3.client('sqs', config=config) if start else None

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reset', '--full-reset', dest='reset', action='store_true', help='Apaga downloads, consolidação, registros e checkpoints locais; não altera AWS')
    parser.add_argument('--scrape', action='store_true', help='Baixa/consolida dados INPE e envia o dataset ao S3, sem enviar mensagens')
    parser.add_argument('--start', action='store_true', help='Lê o último dataset preparado no S3 e envia mensagens ao SQS, sem scraping')
    parser.add_argument('--bucket', default=os.getenv('S3_BUCKET'))
    parser.add_argument('--queue-url', default=os.getenv('SQS_QUEUE_URL'))
    parser.add_argument('--data-dir', type=Path, default=Path(os.getenv('DATA_DIR', 'data')))
    parser.add_argument('--records-per-message', type=int, default=int(os.getenv('RECORDS_PER_MESSAGE', '10')))
    parser.add_argument('--read-interval-seconds', type=float, default=float(os.getenv('READ_INTERVAL_SECONDS', '1')))
    parser.add_argument('--max-files', type=int, default=0, help='0: todos os arquivos disponíveis; N: os N mais recentes')
    parser.add_argument('--prepare-only', action='store_true', help='Somente download e consolidação locais')
    parser.add_argument('--all-countries', action='store_true', help='Desabilita o filtro por Brasil quando existir coluna pais/country')
    args = parser.parse_args(argv)
    if args.prepare_only:
        if args.start:
            parser.error('--prepare-only não pode ser usado com --start')
        args.scrape = True
    if not (args.reset or args.scrape or args.start):
        parser.print_help()
        return 0
    if args.reset and args.start and not args.scrape:
        parser.error('--reset --start exige --scrape para preparar um novo dataset')
    if not 1 <= args.records_per_message <= 10 or args.read_interval_seconds < 0 or args.max_files < 0:
        parser.error('Lote: 1..10; intervalo: >=0; max-files: >=0')
    if (args.start or (args.scrape and not args.prepare_only)) and not args.bucket:
        parser.error('Defina S3_BUCKET ou --bucket')
    if args.start and (not args.queue_url or not args.queue_url.endswith('.fifo')):
        parser.error('Defina SQS_QUEUE_URL ou --queue-url de uma fila .fifo')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if args.reset:
        removed = reset_local_data(args.data_dir / 'state.sqlite3')
        LOG.info('Reset local concluído: %s artefatos removidos; AWS preservada', len(removed))
        if not (args.scrape or args.start):
            return 0
    if args.start and not args.scrape:
        prepared_dataset(args)  # Falha antes de criar estado ou clientes AWS se faltar preparação.
    args.data_dir.mkdir(parents=True, exist_ok=True)
    (args.data_dir / 'raw').mkdir(exist_ok=True)
    # Evita dois produtores compartilharem o mesmo estado local.
    import fcntl
    with (args.data_dir / '.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = connect(args.data_dir / 'state.sqlite3')
        try:
            s3 = sqs = None
            if args.start or (args.scrape and not args.prepare_only):
                s3, sqs = aws_clients(args.start)
            # A rotina roda uma vez; agende execuções externas se precisar repetir.
            try:
                routine(args, db, s3, sqs)
            except Exception:
                LOG.exception('Rodada interrompida; estado preservado para retomada')
                raise
        finally:
            db.close()


if __name__ == '__main__':
    raise SystemExit(main())
