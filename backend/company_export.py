from __future__ import annotations

import hashlib
import sqlite3
from collections import defaultdict


def _stable_company_id(company: str) -> str:
    return f"c-{hashlib.sha1(company.encode('utf-8')).hexdigest()[:12]}"


def load_verified_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        '''
        SELECT
          c.record_id,
          c.company_name,
          c.source_type,
          c.source_title,
          c.source_url,
          c.captured_at,
          c.evidence_summary,
          c.city,
          v.verification_status,
          v.risk_level,
          v.boycott_recommended
        FROM verified_evidence v
        JOIN collected_evidence c ON c.id = v.collected_id
        WHERE v.verification_status != 'error'
          AND c.verification_status != 'error'
        ORDER BY c.company_name, c.captured_at DESC
        '''
    ).fetchall()


def load_pending_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        '''
        SELECT
          record_id,
          company_name,
          source_type,
          source_title,
          source_url,
          captured_at,
          evidence_summary,
          city,
          verification_status,
          risk_level,
          boycott_recommended
        FROM collected_evidence
        WHERE record_id NOT IN (SELECT record_id FROM verified_evidence)
          AND verification_status != 'error'
        ORDER BY company_name, captured_at DESC
        '''
    ).fetchall()


def load_approved_products(conn: sqlite3.Connection) -> dict[str, list[dict[str, str]]]:
    rows = conn.execute(
        '''
        SELECT company_name, product_name, product_category, product_url, confidence
        FROM company_products
        ORDER BY company_name, product_name
        '''
    ).fetchall()

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row['company_name']].append(
            {
                'name': row['product_name'],
                'category': row['product_category'] or '',
                'url': row['product_url'] or '',
                'confidence': row['confidence'] or 'unverified',
                'status': 'approved',
            }
        )
    return grouped


def load_pending_product_suggestions(conn: sqlite3.Connection) -> dict[str, list[dict[str, str]]]:
    try:
        rows = conn.execute(
            '''
            SELECT id, company_name, product_name, product_category, product_url, source_note, review_status
            FROM pending_product_submissions
            WHERE review_status = 'pending'
            ORDER BY company_name, created_at DESC, id DESC
            '''
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if 'no such table' in str(exc):
            return defaultdict(list)
        raise

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row['company_name']].append(
            {
                'id': str(row['id']),
                'name': row['product_name'],
                'category': row['product_category'] or '',
                'url': row['product_url'] or '',
                'confidence': 'pending',
                'status': 'pending',
                'sourceNote': row['source_note'] or '',
            }
        )
    return grouped


def build_company_records(conn: sqlite3.Connection, include_pending: bool = True) -> list[dict]:
    rows = load_verified_rows(conn)
    if include_pending:
        rows = rows + load_pending_rows(conn)

    approved_products = load_approved_products(conn)
    pending_products = load_pending_product_suggestions(conn)

    grouped: dict[str, dict] = {}
    for row in rows:
        company = row['company_name']
        if company not in grouped:
            grouped[company] = {
                'id': _stable_company_id(company),
                'name': company,
                'industry': '待补充',
                'riskLevel': row['risk_level'] or 'medium',
                'verificationStatus': row['verification_status'] or 'pending',
                'boycottRecommended': bool(row['boycott_recommended']),
                'lastUpdated': row['captured_at'] or '',
                'summary': '来自数据库导出，需持续补充多源证据。',
                'products': approved_products.get(company, []),
                'pendingProducts': pending_products.get(company, []),
                'evidence': [],
            }

        grouped[company]['evidence'].append(
            {
                'sourceType': row['source_type'] or 'jobsite',
                'sourceTitle': row['source_title'] or '来源记录',
                'sourceUrl': row['source_url'] or '',
                'capturedAt': row['captured_at'] or '',
                'summary': row['evidence_summary'] or '',
            }
        )

        if row['captured_at'] and row['captured_at'] > grouped[company]['lastUpdated']:
            grouped[company]['lastUpdated'] = row['captured_at']

        if row['verification_status'] == 'verified':
            grouped[company]['verificationStatus'] = 'verified'
        elif row['verification_status'] == 'partial' and grouped[company]['verificationStatus'] == 'pending':
            grouped[company]['verificationStatus'] = 'partial'

    for company, suggestions in pending_products.items():
        if company not in grouped:
            grouped[company] = {
                'id': _stable_company_id(company),
                'name': company,
                'industry': '待补充',
                'riskLevel': 'medium',
                'verificationStatus': 'pending',
                'boycottRecommended': False,
                'lastUpdated': '',
                'summary': '来自数据库导出，需持续补充多源证据。',
                'products': approved_products.get(company, []),
                'pendingProducts': suggestions,
                'evidence': [],
            }
        else:
            grouped[company]['pendingProducts'] = suggestions

    result = list(grouped.values())
    result.sort(key=lambda x: (x['verificationStatus'], x['name']))
    return result
