import csv
from pathlib import Path

import pytest
import producer.main as producer
from test_producer import S3, SQS


class TrackedS3(S3):
    def __init__(self):
        self.uploads = 0
    def upload_file(self, *args, **kwargs):
        self.uploads += 1
        super().upload_file(*args, **kwargs)


@pytest.fixture
def pipeline(monkeypatch, tmp_path):
    names = [f'focos_10min_20261113_{hour:02}00.csv' for hour in range(8)]
    downloaded = []
    def fetch(url, target=None):
        if target is None:
            return ''.join(f'<a href="{name}">{name}</a>' for name in names)
        downloaded.append(target.name)
        hour = names.index(target.name)
        target.write_text(f'lat,lon,satelite,data\n-10,-50,AQUA,2026-11-13 {hour:02}:00:00\n')
    monkeypatch.setattr(producer, 'fetch', fetch)
    s3, sqs = TrackedS3(), SQS()
    clients = []
    def aws_clients(start=False):
        clients.append(start)
        return s3, sqs if start else None
    monkeypatch.setattr(producer, 'aws_clients', aws_clients)
    base = ['--data-dir', str(tmp_path / 'data'), '--bucket', 'bucket', '--queue-url', 'queue.fifo', '--read-interval-seconds', '0']
    return base, s3, sqs, downloaded, clients


def test_scrape_latest_six_uploads_without_sqs(pipeline):
    base, s3, sqs, downloads, clients = pipeline
    producer.main(base + ['--scrape', '--max-files', '6'])
    assert len(downloads) == 6
    assert downloads[0] == 'focos_10min_20261113_0200.csv'
    assert s3.uploads == 1
    assert clients == [False]
    assert sqs.messages == []
    assert (Path(base[1]) / 's3-dataset.json').exists()


def test_start_streams_prepared_s3_without_scraping_or_upload(pipeline, monkeypatch):
    base, s3, sqs, downloads, clients = pipeline
    producer.main(base + ['--scrape', '--max-files', '6'])
    monkeypatch.setattr(producer, 'fetch', lambda *a, **k: pytest.fail('start must not scrape'))
    producer.main(base + ['--start'])
    assert s3.uploads == 1
    assert clients == [False, True]
    assert len(sqs.messages) == 1
    assert len(sqs.messages[0]['records']) == 6
    producer.main(base + ['--start'])
    assert len(sqs.messages) == 1


def test_reset_scrape_start_in_order(pipeline):
    base, s3, sqs, _, _ = pipeline
    directory = Path(base[1])
    (directory / 'raw').mkdir(parents=True)
    (directory / 'raw' / 'old.csv').write_text('old')
    producer.main(base + ['--reset', '--scrape', '--start', '--max-files', '6'])
    assert not (directory / 'raw' / 'old.csv').exists()
    assert s3.uploads == 1
    assert len(sqs.messages[0]['records']) == 6


def test_prepare_only_is_offline_and_reset_needs_no_aws(pipeline):
    base, s3, sqs, _, clients = pipeline
    producer.main(base + ['--prepare-only', '--max-files', '6'])
    assert clients == []
    assert (Path(base[1]) / 'consolidated.csv').exists()
    assert not (Path(base[1]) / 's3-dataset.json').exists()
    assert producer.main(['--reset', '--data-dir', base[1]]) == 0
    assert not (Path(base[1]) / 'state.sqlite3').exists()
    assert not (Path(base[1]) / 'raw').exists()
    assert clients == []


def test_missing_manifest_fails_without_aws(pipeline):
    base, _, _, _, clients = pipeline
    with pytest.raises(ValueError, match='--scrape primeiro'):
        producer.main(base + ['--start'])
    assert clients == []


def test_reset_start_without_scrape_rejected_before_deleting(pipeline):
    base, _, _, _, _ = pipeline
    directory = Path(base[1])
    directory.mkdir()
    saved = directory / 'consolidated.csv'
    saved.write_text('keep')
    with pytest.raises(SystemExit):
        producer.main(base + ['--reset', '--start'])
    assert saved.read_text() == 'keep'


@pytest.mark.parametrize('flag', [['--watch'], ['--poll-seconds', '600']])
def test_watch_loop_was_removed(pipeline, flag):
    base, _, _, _, clients = pipeline
    with pytest.raises(SystemExit):
        producer.main(base + ['--scrape'] + flag)
    assert clients == []
