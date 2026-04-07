#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Batch verify collected evidence from CSV.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--csv', required=True, help='CSV with record_id,status,risk_level,boycott,verifier,note')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
      raise SystemExit(f'CSV not found: {csv_path}')

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    promoted = 0

    try:
        with csv_path.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record_id = (row.get('record_id') or '').strip()
                verifier = (row.get('verifier') or '').strip()
                status = (row.get('status') or 'partial').strip()
                if status not in {'partial', 'verified', 'error'}:
                    status = 'partial'
                risk_level = (row.get('risk_level') or 'medium').strip()
                boycott = (row.get('boycott') or '').strip().lower() in {'1', 'true', 'yes', 'y'}
                note = (row.get('note') or '').strip()

                if not record_id or not verifier:
                    continue

                record = conn.execute(
                    'SELECT id, record_id, company_name FROM collected_evidence WHERE record_id = ?',
                    (record_id,),
                ).fetchone()
                if record is None:
                    continue

                conn.execute(
                    '''
                    INSERT INTO verified_evidence (
                      collected_id, record_id, company_name, verification_status,
                      risk_level, boycott_recommended, verifier, verification_note, verified_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE)
                    ON CONFLICT(record_id) DO UPDATE SET
                      company_name=excluded.company_name,
                      verification_status=excluded.verification_status,
                      risk_level=excluded.risk_level,
                      boycott_recommended=excluded.boycott_recommended,
                      verifier=excluded.verifier,
                      verification_note=excluded.verification_note,
                      verified_at=excluded.verified_at
                    ''',
                    (record['id'], record['record_id'], record['company_name'], status, risk_level, 1 if boycott else 0, verifier, note),
                )
                conn.execute(
                    '''
                    UPDATE collected_evidence
                    SET verification_status = ?, risk_level = ?, boycott_recommended = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE record_id = ?
                    ''',
                    (status, risk_level, 1 if boycott else 0, record_id),
                )
                promoted += 1
        conn.commit()
    finally:
        conn.close()

    print(f'[DONE] Batch verified {promoted} records')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
