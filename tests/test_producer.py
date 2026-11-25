import csv
import io
import json
from pathlib import Path

import pytest

from common.records import from_csv, record, timestamp
from producer.main import Listing, batches, connect, consolidate, ingest, publish, stream_dataset


def test_timezone_and_identity():
    assert timestamp('2026-11-10 01:30:00') == '2026-11-10T01:30:00.000000Z'
    assert record('2026-11-10T01:30:00Z', 'aqua', -10, -50) == record('2026-11-09T22:30:00-03:00', ' AQUA ', '-10.0', '-50')


@pytest.mark.parametrize('lat,lon', [(91, 0), (0, 181), ('nan', 0), (0, 'inf')])
def test_invalid_coordinates(lat, lon):
    with pytest.raises(ValueError):
        record('2026-11-10', 'AQUA', lat, lon)


def test_csv_schemas():
    a = from_csv({'DataHora': '2026-11-10 01:00:00', 'Satélite': 'AQUA', 'Latitude': '-10', 'Longitude': '-50', 'País': 'Brasil'})
    b = from_csv({'data_hora_gmt': '2026-11-10 01:00:00', 'satelite': 'AQUA', 'lat': '-10', 'lon': '-50', 'pais': 'Brasil'})
    assert a == b
    assert from_csv({'pais': 'Peru'}) is None


def test_listing_excludes_external_links():
    parser = Listing()
    parser.feed('<a href="focos_10min_20261110_0010.csv">x</a><a href="../bad.csv">bad</a><a href="https://evil/focos_10min_20261110_0010.csv">bad</a>')
    assert parser.files == {'focos_10min_20261110_0010.csv'}


def test_consolidation_sorted_deduplicated_and_atomic(tmp_path):
    db = connect(tmp_path / 'state.db')
    source = tmp_path / 'input.csv'
    source.write_text('DataHora,Satelite,Latitude,Longitude,Pais\n2026-11-10 02:00:00,AQUA,-10,-50,Brasil\n2026-11-10 01:00:00,TERRA,-11,-51,Brasil\n2026-11-10 02:00:00,AQUA,-10,-50,Brasil\n')
    assert ingest(db, source) == 2
    assert ingest(db, source) == 0
    path, digest = consolidate(db, tmp_path)
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert rows[0]['satellite'] == 'TERRA'
    assert len(digest) == 64
    source.write_text('DataHora,Satelite,Latitude,Longitude,Pais\n2026-11-11,AQUA,-10,-50,Brasil\n2026-11-11,AQUA,999,-50,Brasil\n')
    with pytest.raises(ValueError):
        ingest(db, source)
    assert db.execute('SELECT count(*) FROM records').fetchone()[0] == 2
    db.close()


def test_empty_and_invalid_header(tmp_path):
    db = connect(tmp_path / 'state.db')
    source = tmp_path / 'empty.csv'
    source.write_text('Nenhum foco encontrado')
    assert ingest(db, source) == 0
    source.write_text('<html>Erro no servidor</html>')
    with pytest.raises(ValueError, match='cabeçalho'):
        ingest(db, source)
    db.close()


@pytest.mark.parametrize('size', [1, 3, 10])
def test_batch_limit(size):
    groups = list(batches(range(23), size))
    assert all(1 <= len(group) <= size for group in groups)
    assert [item for group in groups for item in group] == list(range(23))


@pytest.mark.parametrize('size', [0, 11])
def test_reject_invalid_batch(size):
    with pytest.raises(ValueError):
        list(batches([], size))


class S3:
    def upload_file(self, path, bucket, key, **kwargs):
        self.content = Path(path).read_bytes()
    def get_object(self, **kwargs):
        return {'Body': io.BytesIO(self.content)}


class SQS:
    def __init__(self, fail=False):
        self.messages = []
        self.fail = fail
    def send_message(self, **kwargs):
        if self.fail:
            raise RuntimeError('Falha simulada')
        self.messages.append(json.loads(kwargs['MessageBody']))
        assert kwargs['MessageGroupId'] == 'inpe-chronological'


def test_stream_and_resume(tmp_path):
    db = connect(tmp_path / 'state.db')
    with db:
        for i in range(23):
            item = record(f'2026-11-10T00:00:{i:02}Z', 'AQUA', -10, -50)
            db.execute('INSERT INTO records VALUES (?,?,?)', (item['id'], item['date'], json.dumps(item)))
    path, digest = consolidate(db, tmp_path)
    s3, sqs = S3(), SQS()
    assert publish(db, s3, sqs, 'bucket', 'queue.fifo', path, digest, 10, 0) == 23
    assert [len(m['records']) for m in sqs.messages] == [10, 10, 3]
    assert publish(db, s3, sqs, 'bucket', 'queue.fifo', path, digest, 10, 0) == 0
    assert len(sqs.messages) == 3
    db.close()


def test_failed_send_does_not_checkpoint(tmp_path):
    db = connect(tmp_path / 'state.db')
    item = record('2026-11-10', 'AQUA', -10, -50)
    with db:
        db.execute('INSERT INTO records VALUES (?,?,?)', (item['id'], item['date'], json.dumps(item)))
    path, digest = consolidate(db, tmp_path)
    with pytest.raises(RuntimeError):
        publish(db, S3(), SQS(fail=True), 'bucket', 'queue.fifo', path, digest, 10, 0)
    assert db.execute('SELECT count(*) FROM sent').fetchone()[0] == 0
    db.close()


def test_inpe_four_column_csv_without_country(tmp_path, caplog):
    db = connect(tmp_path / 'state.db')
    source = tmp_path / 'focos_10min_20261113_2000.csv'
    source.write_text('lat,lon,satelite,data\n  -4.678900, -80.572000,GOES-19,2026-11-13 19:40:00\n  -4.697300, -80.681400,GOES-19,2026-11-13 19:40:00\n')
    assert ingest(db, source) == 2
    assert 'sem coluna de país' in caplog.text
    path, _ = consolidate(db, tmp_path)
    with path.open() as stream:
        rows = list(csv.DictReader(stream))
    assert rows[0]['date'] == '2026-11-13T19:40:00.000000Z'
    assert rows[0]['satellite'] == 'GOES-19'
    assert all(float(r['longitude']) < -80 for r in rows)
    assert ingest(db, source) == 0
    db.close()


class TrackedBody(io.BytesIO):
    """Corpo S3 que registra quantos bytes já foram lidos do objeto."""
    def __init__(self, content):
        super().__init__(content)
        self.size = len(content)
        self.max_read = 0
    def _track(self, result):
        self.max_read = max(self.max_read, self.tell())
        return result
    def read(self, *args):
        return self._track(super().read(*args))
    def read1(self, *args):
        return self._track(super().read1(*args))
    def readinto(self, *args):
        return self._track(super().readinto(*args))


def test_stream_sends_first_message_before_reading_whole_s3_object(tmp_path):
    db = connect(tmp_path / 'state.db')
    with db:
        for i in range(5000):
            item = record(f'2026-11-10T00:{i // 60 % 60:02}:{i % 60:02}Z', f'SAT{i}', -10, -50)
            db.execute('INSERT INTO records VALUES (?,?,?)', (item['id'], item['date'], json.dumps(item)))
    path, _ = consolidate(db, tmp_path)
    body = TrackedBody(path.read_bytes())
    assert body.size > 100 * 1024  # bem maior que o buffer de leitura do TextIOWrapper

    class StreamingS3:
        def get_object(self, **kwargs):
            return {'Body': body}

    class RecordingSQS(SQS):
        def __init__(self):
            super().__init__()
            self.read_at_send = []
        def send_message(self, **kwargs):
            self.read_at_send.append(body.max_read)
            super().send_message(**kwargs)

    sqs = RecordingSQS()
    assert stream_dataset(db, StreamingS3(), sqs, 'bucket', 'queue.fifo', 'consolidated/x.csv', 10, 0) == 5000
    # 1ª mensagem saiu quando só uma fração pequena do objeto tinha sido lida.
    assert sqs.read_at_send[0] < body.size // 10
    assert sqs.read_at_send == sorted(sqs.read_at_send)
    assert body.max_read == body.size
    db.close()
