from pathlib import Path
from datetime import datetime, timezone
import json, hashlib, shutil, os, csv, argparse, uuid
import requests

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data'; RAW = ROOT / 'raw'; STATE = ROOT / 'state'; OUTPUTS = ROOT / 'outputs'
API_URL = 'http://127.0.0.1:8000/api/events'

# Header taken verbatim from templates/pipeline_run_log_template.csv
RUN_LOG_PATH = OUTPUTS / 'pipeline_run_log.csv'
RUN_LOG_HEADER = [
    'run_id', 'started_at', 'finished_at', 'status', 'source',
    'records_read', 'records_written', 'duplicates_removed',
    'watermark_before', 'watermark_after', 'error_message',
]

SOURCE_FILES = ['customers.csv', 'orders.json', 'products.parquet']


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def load_watermark():
    p = STATE / 'api_watermark.json'
    if not p.exists():
        return None
    return json.loads(p.read_text())['updated_at']


def save_watermark(value):
    STATE.mkdir(exist_ok=True)
    final_path = STATE / 'api_watermark.json'
    tmp_path = STATE / 'api_watermark.json.tmp'
    tmp_path.write_text(json.dumps({'updated_at': value}, indent=2))
    os.replace(tmp_path, final_path)  # atomic on the same filesystem


# ---------------------------------------------------------------------------
# File ingestion (Task 3.1)
# ---------------------------------------------------------------------------

def _load_manifest_hashes(manifest_path):
    if not manifest_path.exists():
        return set()
    hashes = set()
    with manifest_path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                hashes.add(json.loads(line)['sha256'])
    return hashes


def ingest_files():
    """
    Copy customers.csv, orders.json, and products.parquet to raw/files/
    without modifying source content. Dedup is content-hash based: a file
    whose sha256 already appears in the manifest is skipped, so a rerun
    with unchanged sources creates no duplicate copies and no duplicate
    manifest entries.
    """
    raw_files_dir = RAW / 'files'
    raw_files_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = raw_files_dir / 'manifest.jsonl'
    already_ingested = _load_manifest_hashes(manifest_path)

    results = []
    for filename in SOURCE_FILES:
        source_path = DATA / filename
        if not source_path.exists():
            results.append({'file': filename, 'status': 'missing'})
            continue

        digest = sha256_file(source_path)
        if digest in already_ingested:
            results.append({'file': filename, 'status': 'skipped_duplicate_hash', 'sha256': digest})
            continue

        dest_name = f"{source_path.stem}__{digest[:8]}{source_path.suffix}"
        dest_path = raw_files_dir / dest_name
        shutil.copy2(source_path, dest_path)

        entry = {
            'source_file': filename,
            'raw_file': dest_name,
            'ingested_at': utc_now(),
            'bytes': source_path.stat().st_size,
            'sha256': digest,
        }
        with manifest_path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
        already_ingested.add(digest)
        results.append({'file': filename, 'status': 'ingested', 'sha256': digest})

    for r in results:
        print(f"{r['status']:>24}  {r['file']}")
    return results


# ---------------------------------------------------------------------------
# API ingestion (Task 3.2 - 3.4)
# ---------------------------------------------------------------------------

def fetch_api_page(page, per_page=20, updated_after=None):
    params = {'page': page, 'per_page': per_page}
    if updated_after:
        params['updated_after'] = updated_after
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _events_path():
    return RAW / 'api' / 'events.jsonl'


def _load_existing_events():
    path = _events_path()
    if not path.exists():
        return {}
    existing = {}
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                record = json.loads(line)
                existing[record['event_id']] = record
    return existing


def _atomic_write_events(records_by_id):
    api_dir = RAW / 'api'
    api_dir.mkdir(parents=True, exist_ok=True)
    final_path = _events_path()
    tmp_path = api_dir / 'events.jsonl.tmp'
    with tmp_path.open('w', encoding='utf-8') as f:
        for event_id in sorted(records_by_id):
            f.write(json.dumps(records_by_id[event_id]) + '\n')
    os.replace(tmp_path, final_path)  # atomic on the same filesystem


def ingest_api(per_page=20):
    """
    1) Read the watermark and request only records updated after it.
    2) Follow pagination until has_more is False.
    3) Tag each record with _ingested_at / _source.
    4) Merge against whatever is already durably stored, keeping -- per
       event_id -- whichever record (existing or newly fetched) has the
       greatest updated_at. This is what makes the merge safe even when a
       later run re-observes an event_id already persisted from an earlier run.
    5) Write the merged result to raw/api/events.jsonl atomically.
    6) Only after that write succeeds, recompute and persist the watermark
       as the greatest updated_at across the full merged set. If any page
       request raises before this point, the watermark is never touched.
    """
    watermark = load_watermark()
    page = 1
    fetched_records = []

    while True:
        payload = fetch_api_page(page, per_page=per_page, updated_after=watermark)
        for record in payload.get('items', []):
            record = dict(record)
            record['_ingested_at'] = utc_now()
            record['_source'] = 'local_api_events'
            fetched_records.append(record)

        if not payload.get('has_more'):
            break
        page = payload.get('next_page') or (page + 1)

    merged_by_id = _load_existing_events()
    duplicates_removed = 0
    for record in fetched_records:
        event_id = record['event_id']
        current = merged_by_id.get(event_id)
        if current is not None:
            # event_id already seen (either from a prior run or earlier in
            # this same batch) -- this counts as a duplicate regardless of
            # which record ultimately wins.
            duplicates_removed += 1
        if current is None or record['updated_at'] > current['updated_at']:
            merged_by_id[event_id] = record

    _atomic_write_events(merged_by_id)

    if merged_by_id:
        new_watermark = max(r['updated_at'] for r in merged_by_id.values())
        save_watermark(new_watermark)

    print(
        f"pages_processed={page} fetched={len(fetched_records)} "
        f"duplicates_removed={duplicates_removed} "
        f"total_logical_events={len(merged_by_id)}"
    )
    return {
        'fetched': len(fetched_records),
        'duplicates_removed': duplicates_removed,
        'total_logical_events': len(merged_by_id),
    }


# ---------------------------------------------------------------------------
# Run-log evidence (Task 3.5)
# ---------------------------------------------------------------------------

def generate_run_id():
    return f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}_{uuid.uuid4().hex[:6]}"


def _ensure_run_log():
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    if not RUN_LOG_PATH.exists():
        with RUN_LOG_PATH.open('w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=RUN_LOG_HEADER).writeheader()


def _append_run_log(row):
    """Append one row, filling any column the caller didn't supply with ''."""
    _ensure_run_log()
    full_row = {key: row.get(key, '') for key in RUN_LOG_HEADER}
    with RUN_LOG_PATH.open('a', newline='', encoding='utf-8') as f:
        csv.DictWriter(f, fieldnames=RUN_LOG_HEADER).writerow(full_row)


def run_files_stage():
    """Run file ingestion once and append exactly one run-log row for it."""
    run_id = generate_run_id()
    started_at = utc_now()
    try:
        results = ingest_files()
        attempted = [r for r in results if r['status'] != 'missing']
        written = [r for r in results if r['status'] == 'ingested']
        skipped = [r for r in results if r['status'] == 'skipped_duplicate_hash']
        _append_run_log({
            'run_id': run_id, 'started_at': started_at, 'finished_at': utc_now(),
            'status': 'success', 'source': 'files',
            'records_read': len(attempted), 'records_written': len(written),
            'duplicates_removed': len(skipped),
        })
        return results
    except Exception as exc:
        _append_run_log({
            'run_id': run_id, 'started_at': started_at, 'finished_at': utc_now(),
            'status': 'failed', 'source': 'files', 'error_message': str(exc),
        })
        raise


def run_api_stage(per_page=20):
    """Run API ingestion once and append exactly one run-log row for it."""
    run_id = generate_run_id()
    started_at = utc_now()
    watermark_before = load_watermark()
    try:
        result = ingest_api(per_page=per_page)
        _append_run_log({
            'run_id': run_id, 'started_at': started_at, 'finished_at': utc_now(),
            'status': 'success', 'source': 'api',
            'records_read': result['fetched'],
            'records_written': result['total_logical_events'],
            'duplicates_removed': result['duplicates_removed'],
            'watermark_before': watermark_before or '',
            'watermark_after': load_watermark() or '',
        })
        return result
    except Exception as exc:
        # ingest_api() never calls save_watermark until after a successful
        # atomic write, so on any exception the watermark is guaranteed
        # unchanged. watermark_after is read fresh here (not copied from
        # watermark_before) precisely so the log itself is the evidence of
        # that, rather than an assumption baked into the log line.
        _append_run_log({
            'run_id': run_id, 'started_at': started_at, 'finished_at': utc_now(),
            'status': 'failed', 'source': 'api', 'error_message': str(exc),
            'watermark_before': watermark_before or '',
            'watermark_after': load_watermark() or '',
        })
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', nargs='?', default='all', choices=['files', 'api', 'all'])
    args = parser.parse_args()

    if args.stage in ('files', 'all'):
        run_files_stage()
    if args.stage in ('api', 'all'):
        run_api_stage()