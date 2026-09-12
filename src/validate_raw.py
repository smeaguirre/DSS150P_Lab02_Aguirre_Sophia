"""
Task 3.6 - Automated idempotency validation (validate_raw.py).

Run this after one or more pipeline executions to check, programmatically
rather than by manual inspection, that the raw area holds no duplicate
logical records. Exits non-zero (and prints which assertion failed) if
anything is wrong, so it can be wired into a CI step or just run by hand.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / 'raw'


def check_file_manifest_no_duplicate_hashes():
    manifest_path = RAW / 'files' / 'manifest.jsonl'
    if not manifest_path.exists():
        print('  [skip] no manifest.jsonl yet')
        return True

    hashes = []
    with manifest_path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                hashes.append(json.loads(line)['sha256'])

    duplicates = len(hashes) - len(set(hashes))
    if duplicates:
        print(f'  [FAIL] manifest.jsonl contains {duplicates} duplicate sha256 entries')
        return False

    print(f'  [ok] manifest.jsonl: {len(hashes)} entries, all sha256 values unique')
    return True


def check_raw_files_match_manifest_count():
    manifest_path = RAW / 'files' / 'manifest.jsonl'
    raw_files_dir = RAW / 'files'
    if not manifest_path.exists():
        print('  [skip] no manifest.jsonl yet')
        return True

    manifest_raw_names = set()
    with manifest_path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                manifest_raw_names.add(json.loads(line)['raw_file'])

    actual_files = {p.name for p in raw_files_dir.glob('*') if p.name != 'manifest.jsonl'}

    if manifest_raw_names != actual_files:
        print(f'  [FAIL] manifest lists {len(manifest_raw_names)} raw files but '
              f'{len(actual_files)} exist on disk -- they should match exactly')
        print(f'         only in manifest: {manifest_raw_names - actual_files}')
        print(f'         only on disk:     {actual_files - manifest_raw_names}')
        return False

    print(f'  [ok] raw/files/ contents match manifest exactly ({len(actual_files)} files)')
    return True


def check_api_events_no_duplicate_event_id():
    events_path = RAW / 'api' / 'events.jsonl'
    if not events_path.exists():
        print('  [skip] no events.jsonl yet')
        return True

    event_ids = []
    with events_path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                event_ids.append(json.loads(line)['event_id'])

    duplicates = len(event_ids) - len(set(event_ids))
    if duplicates:
        print(f'  [FAIL] events.jsonl contains {duplicates} duplicate event_id entries '
              f'-- exactly one logical record per event_id is required')
        return False

    print(f'  [ok] events.jsonl: {len(event_ids)} logical events, all event_id values unique')
    return True


def main():
    print('Running idempotency validation checks...')
    checks = [
        check_file_manifest_no_duplicate_hashes,
        check_raw_files_match_manifest_count,
        check_api_events_no_duplicate_event_id,
    ]
    results = [check() for check in checks]

    if all(results):
        print('\nAll checks passed.')
        return 0
    else:
        print('\nOne or more checks FAILED.')
        return 1


if __name__ == '__main__':
    sys.exit(main())