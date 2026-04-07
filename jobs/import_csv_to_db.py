#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

INSERT_SQL = '''
INSERT INTO collected_evidence (
  record_id, company_name, uscc_or_entity_id, source_type, source_platform,
  source_url, source_title, published_at, captured_at, city, job_title,
  evidence_quote, evidence_summary, screenshot_path, collector,
  verification_status, risk_level, boycott_recommended, notes
) VALUES (
  :record_id, :company_name, :uscc_or_entity_id, :source_type, :source_platform,
  :source_url, :source_title, :published_at, :captured_at, :city, :job_title,
  :evidence_quote, :evidence_summary, :screenshot_path, :collector,
  :verification_status, :risk_level, :boycott_recommended, :notes
)
ON CONFLICT(record_id) DO UPDATE SET
  company_name=excluded.company_name,
  uscc_or_entity_id=excluded.uscc_or_entity_id,
  source_type=excluded.source_type,
  source_platform=excluded.source_platform,
  source_url=excluded.source_url,
  source_title=excluded.source_title,
  published_at=excluded.published_at,
  captured_at=excluded.captured_at,
  city=excluded.city,
  job_title=excluded.job_title,
  evidence_quote=excluded.evidence_quote,
  evidence_summary=excluded.evidence_summary,
  screenshot_path=excluded.screenshot_path,
  collector=excluded.collector,
  verification_status=excluded.verification_status,
  risk_level=excluded.risk_level,
  boycott_recommended=excluded.boycott_recommended,
  notes=excluded.notes,
  updated_at=CURRENT_TIMESTAMP
'''


def normalize_row(row: dict[str, str]) -> dict[str, str | int]:
    mapped = {k: (row.get(k, '') or '').strip() for k in row}
    b = mapped.get('boycott_recommended', '').lower()
    boycott = 1 if b in {'1', 'true', 'yes', 'y'} else 0
    mapped['boycott_recommended'] = boycott

    if not mapped.get('verification_status'):
        mapped['verification_status'] = 'pending'
    if not mapped.get('risk_level'):
        mapped['risk_level'] = 'medium'
    if not mapped.get('source_type'):
        mapped['source_type'] = 'jobsite'
    if not mapped.get('source_platform'):
        mapped['source_platform'] = 'unknown'

    return mapped


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Import intake CSV into SQLite collected_evidence table.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--csv', required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f'CSV not found: {csv_path}')

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    inserted = 0
    try:
        with csv_path.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data = normalize_row(row)
                conn.execute(INSERT_SQL, data)
                inserted += 1
        conn.commit()
    finally:
        conn.close()

    print(f'[DONE] Upserted {inserted} rows from {csv_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
