#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import sqlite3

UPSERT_VERIFIED = '''
INSERT INTO verified_evidence (
  collected_id, record_id, company_name, verification_status,
  risk_level, boycott_recommended, verifier, verification_note, verified_at
) VALUES (
  :collected_id, :record_id, :company_name, :verification_status,
  :risk_level, :boycott_recommended, :verifier, :verification_note, :verified_at
)
ON CONFLICT(record_id) DO UPDATE SET
  company_name=excluded.company_name,
  verification_status=excluded.verification_status,
  risk_level=excluded.risk_level,
  boycott_recommended=excluded.boycott_recommended,
  verifier=excluded.verifier,
  verification_note=excluded.verification_note,
  verified_at=excluded.verified_at
'''


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Promote collected evidence rows into verified_evidence table.')
    p.add_argument('--db', default='data/forbidden_company.db')
    p.add_argument('--record-ids', required=True, help='Comma-separated record_ids')
    p.add_argument('--status', choices=['partial', 'verified', 'error'], default='partial')
    p.add_argument('--risk-level', choices=['low', 'medium', 'high'], default='medium')
    p.add_argument('--boycott', action='store_true')
    p.add_argument('--verifier', required=True)
    p.add_argument('--note', default='')
    return p.parse_args()


def main() -> int:
    args = parse_args()
    record_ids = [x.strip() for x in args.record_ids.split(',') if x.strip()]
    if not record_ids:
        raise SystemExit('No record_ids provided')

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    verified_at = dt.date.today().isoformat()
    promoted = 0

    try:
        for record_id in record_ids:
            row = conn.execute(
                'SELECT id, record_id, company_name FROM collected_evidence WHERE record_id = ?',
                (record_id,),
            ).fetchone()
            if row is None:
                print(f'[WARN] record_id not found: {record_id}')
                continue

            payload = {
                'collected_id': row['id'],
                'record_id': row['record_id'],
                'company_name': row['company_name'],
                'verification_status': args.status,
                'risk_level': args.risk_level,
                'boycott_recommended': 1 if args.boycott else 0,
                'verifier': args.verifier,
                'verification_note': args.note,
                'verified_at': verified_at,
            }
            conn.execute(UPSERT_VERIFIED, payload)
            conn.execute(
                '''
                UPDATE collected_evidence
                SET verification_status = ?,
                    risk_level = ?,
                    boycott_recommended = ?,
                    notes = CASE
                      WHEN notes IS NULL OR notes = '' THEN ?
                      ELSE notes || ' | ' || ?
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE record_id = ?
                ''',
                (args.status, args.risk_level, 1 if args.boycott else 0, args.note, args.note, record_id),
            )
            promoted += 1
        conn.commit()
    finally:
        conn.close()

    print(f'[DONE] Promoted {promoted} records to verified_evidence')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
