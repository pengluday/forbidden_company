from __future__ import annotations

import datetime as dt
import hashlib
import re
import sqlite3
from collections import defaultdict


def _stable_company_id(company: str) -> str:
    return f"c-{hashlib.sha1(company.encode('utf-8')).hexdigest()[:12]}"


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


AGE_LIMIT_RE = re.compile(r'35\s*(?:周\s*岁|周岁|岁)\s*以下')


def _row_text(row: sqlite3.Row | dict) -> str:
    parts = []
    key_pairs = (
        ('source_title', 'sourceTitle'),
        ('job_title', 'jobTitle'),
        ('evidence_quote', 'evidenceQuote'),
        ('evidence_summary', 'summary'),
        ('notes', 'notes'),
    )
    for snake_key, camel_key in key_pairs:
        if isinstance(row, sqlite3.Row):
            value = row[snake_key]
        else:
            value = row.get(snake_key) or row.get(camel_key)
        if value:
            parts.append(str(value))
    return '\n'.join(parts)


def _matches_age_limit(row: sqlite3.Row | dict) -> bool:
    return bool(AGE_LIMIT_RE.search(_row_text(row)))


def derive_auto_age_review(company_name: str, evidence_rows: list[sqlite3.Row | dict]) -> dict:
    matching_rows = [row for row in evidence_rows if _matches_age_limit(row)]
    matching_count = len(matching_rows)

    if matching_count >= 3:
        conclusion = 'clear'
        confidence = 'high'
        risk_level = 'high'
        boycott_recommended = True
        evidence_level = 'L3'
        reason = f'发现 {matching_count} 条证据包含“35岁以下/35周岁以下”，达到明确风险阈值。'
    elif matching_count == 1:
        conclusion = 'suspected'
        confidence = 'low'
        risk_level = 'medium'
        boycott_recommended = True
        evidence_level = 'L1'
        reason = '发现 1 条证据包含“35岁以下/35周岁以下”，暂判定为疑似风险。'
    elif matching_count == 2:
        conclusion = 'suspected'
        confidence = 'low'
        risk_level = 'medium'
        boycott_recommended = True
        evidence_level = 'L1'
        reason = '发现 2 条证据包含“35岁以下/35周岁以下”，暂判定为疑似风险。'
    else:
        conclusion = 'insufficient'
        confidence = 'low'
        risk_level = 'low'
        boycott_recommended = False
        evidence_level = 'L2' if matching_count == 2 else 'L1'
        reason = '当前未发现包含“35岁以下/35周岁以下”的证据，暂按证据不足处理。'

    reviewed_at = dt.datetime.now().replace(microsecond=0).isoformat(sep=' ')
    latest_evidence_at = ''
    if evidence_rows:
        captured_values = [str(row['captured_at'] or '') if isinstance(row, sqlite3.Row) else str(row.get('captured_at') or row.get('capturedAt') or '') for row in evidence_rows]
        captured_values = [value for value in captured_values if value]
        if captured_values:
            latest_evidence_at = max(captured_values)

    return {
        'companyName': company_name,
        'conclusion': conclusion,
        'confidence': confidence,
        'riskLevel': risk_level,
        'boycottRecommended': boycott_recommended,
        'evidenceLevel': evidence_level,
        'reason': reason,
        'evidenceCount': matching_count,
        'reviewedAt': reviewed_at,
        'lastEvidenceAt': latest_evidence_at,
        'productName': '',
        'businessLine': '',
    }


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
          c.evidence_quote,
          c.evidence_summary,
          c.job_title,
          c.notes,
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
          evidence_quote,
          evidence_summary,
          job_title,
          notes,
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


def load_product_lines(conn: sqlite3.Connection) -> dict[str, list[dict[str, str]]]:
    if not table_exists(conn, 'company_product_lines'):
        return defaultdict(list)

    rows = conn.execute(
        '''
        SELECT company_name, product_name, business_line, product_category, mapping_status, mapping_source, confidence
        FROM company_product_lines
        ORDER BY company_name, product_name
        '''
    ).fetchall()

    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row['company_name']].append(
            {
                'name': row['product_name'],
                'businessLine': row['business_line'] or '',
                'category': row['product_category'] or '',
                'mappingStatus': row['mapping_status'] or 'unverified',
                'mappingSource': row['mapping_source'] or '',
                'confidence': row['confidence'] or 'low',
            }
        )
    return grouped


def load_latest_conclusions(conn: sqlite3.Connection) -> dict[str, dict]:
    if not table_exists(conn, 'company_conclusions'):
        return {}

    rows = conn.execute(
        '''
        SELECT company_name, product_name, business_line, age_risk_conclusion, age_risk_confidence,
               reason, evidence_count, evidence_level, reviewer, reviewed_at, created_at
        FROM company_conclusions
        ORDER BY company_name, created_at DESC, id DESC
        '''
    ).fetchall()

    grouped: dict[str, dict] = {}
    for row in rows:
        company = row['company_name']
        if company in grouped:
            continue
        grouped[company] = {
            'productName': row['product_name'] or '',
            'businessLine': row['business_line'] or '',
            'conclusion': row['age_risk_conclusion'] or 'insufficient',
            'confidence': row['age_risk_confidence'] or 'low',
            'reason': row['reason'] or '',
            'evidenceCount': int(row['evidence_count'] or 0),
            'evidenceLevel': row['evidence_level'] or 'L1',
            'reviewer': row['reviewer'] or '',
            'reviewedAt': row['reviewed_at'] or '',
            'createdAt': row['created_at'] or '',
        }
    return grouped


def load_company_summaries(conn: sqlite3.Connection) -> dict[str, dict]:
    if not table_exists(conn, 'companies'):
        return {}

    rows = conn.execute(
        '''
        SELECT company_name, product_line, age_risk_conclusion, age_risk_confidence, risk_level,
               conclusion_reason, evidence_level, last_evidence_at, last_reviewed_at, boycott_recommended
        FROM companies
        ORDER BY updated_at DESC, company_name ASC
        '''
    ).fetchall()

    summaries: dict[str, dict] = {}
    for row in rows:
        summaries[row['company_name']] = {
            'productLine': row['product_line'] or '',
            'ageRiskConclusion': row['age_risk_conclusion'] or 'insufficient',
            'ageRiskConfidence': row['age_risk_confidence'] or 'low',
            'riskLevel': row['risk_level'] or 'medium',
            'conclusionReason': row['conclusion_reason'] or '',
            'evidenceLevel': row['evidence_level'] or 'L1',
            'lastEvidenceAt': row['last_evidence_at'] or '',
            'lastReviewedAt': row['last_reviewed_at'] or '',
            'boycottRecommended': bool(row['boycott_recommended']),
        }
    return summaries


def sync_company_summaries(conn: sqlite3.Connection, records: list[dict]) -> None:
    if not table_exists(conn, 'companies'):
        return

    for record in records:
        company_name = (record.get('name') or '').strip()
        if not company_name:
            continue
        product_line = (record.get('productLine') or '').strip()
        age_risk_conclusion = (record.get('ageRiskConclusion') or 'insufficient').strip()
        age_risk_confidence = (record.get('ageRiskConfidence') or 'low').strip()
        risk_level = (record.get('riskLevel') or 'medium').strip()
        conclusion_reason = (record.get('conclusionReason') or '').strip()
        evidence_level = (record.get('evidenceLevel') or 'L1').strip()
        last_evidence_at = (record.get('lastEvidenceAt') or '').strip()
        last_reviewed_at = (record.get('lastReviewedAt') or '').strip()
        boycott_recommended = 1 if record.get('boycottRecommended') else 0

        conn.execute(
            '''
            INSERT INTO companies (
              company_name, product_line, age_risk_conclusion, age_risk_confidence,
              risk_level, conclusion_reason, evidence_level, last_evidence_at,
              last_reviewed_at, boycott_recommended, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(company_name) DO UPDATE SET
              product_line=excluded.product_line,
              age_risk_conclusion=excluded.age_risk_conclusion,
              age_risk_confidence=excluded.age_risk_confidence,
              risk_level=excluded.risk_level,
              conclusion_reason=excluded.conclusion_reason,
              evidence_level=excluded.evidence_level,
              last_evidence_at=excluded.last_evidence_at,
              last_reviewed_at=excluded.last_reviewed_at,
              boycott_recommended=excluded.boycott_recommended,
              updated_at=CURRENT_TIMESTAMP
            ''',
            (
                company_name,
                product_line,
                age_risk_conclusion,
                age_risk_confidence,
                risk_level,
                conclusion_reason,
                evidence_level,
                last_evidence_at,
                last_reviewed_at,
                boycott_recommended,
            ),
        )


def build_company_records(conn: sqlite3.Connection, include_pending: bool = True) -> list[dict]:
    rows = load_verified_rows(conn)
    if include_pending:
        rows = rows + load_pending_rows(conn)

    approved_products = load_approved_products(conn)
    pending_products = load_pending_product_suggestions(conn)
    product_lines = load_product_lines(conn)
    company_summaries = load_company_summaries(conn)

    grouped: dict[str, dict] = {}
    for row in rows:
        company = row['company_name']
        if company not in grouped:
            product_line_list = product_lines.get(company, [])
            summary = company_summaries.get(company, {})
            grouped[company] = {
                'id': _stable_company_id(company),
                'name': company,
                'industry': '待补充',
                'riskLevel': summary.get('riskLevel') or row['risk_level'] or 'medium',
                'verificationStatus': row['verification_status'] or 'pending',
                'boycottRecommended': summary.get('boycottRecommended', bool(row['boycott_recommended'])),
                'lastUpdated': row['captured_at'] or '',
                'lastEvidenceAt': summary.get('lastEvidenceAt') or row['captured_at'] or '',
                'lastReviewedAt': summary.get('lastReviewedAt') or '',
                'summary': '来自数据库导出，需持续补充多源证据。',
                'products': approved_products.get(company, []),
                'productLines': product_line_list,
                'productLine': summary.get('productLine') or (product_line_list[0]['businessLine'] if product_line_list else ''),
                'ageRiskConclusion': summary.get('ageRiskConclusion') or 'insufficient',
                'ageRiskConfidence': summary.get('ageRiskConfidence') or 'low',
                'conclusionReason': summary.get('conclusionReason') or '',
                'evidenceLevel': summary.get('evidenceLevel') or 'L1',
                'conclusionProductName': '',
                'conclusionBusinessLine': '',
                'conclusionEvidenceCount': 0,
                'pendingProducts': pending_products.get(company, []),
                'evidence': [],
            }

        grouped[company]['evidence'].append(
            {
                'sourceType': row['source_type'] or 'jobsite',
                'sourceTitle': row['source_title'] or '来源记录',
                'sourceUrl': row['source_url'] or '',
                'capturedAt': row['captured_at'] or '',
                'evidenceQuote': row['evidence_quote'] or '',
                'summary': row['evidence_summary'] or '',
                'jobTitle': row['job_title'] or '',
                'notes': row['notes'] or '',
            }
        )

        if row['captured_at'] and row['captured_at'] > grouped[company]['lastUpdated']:
            grouped[company]['lastUpdated'] = row['captured_at']

        if row['verification_status'] == 'verified':
            grouped[company]['verificationStatus'] = 'verified'
        elif row['verification_status'] == 'partial' and grouped[company]['verificationStatus'] == 'pending':
            grouped[company]['verificationStatus'] = 'partial'

        if row['captured_at'] and (
            not grouped[company].get('lastEvidenceAt')
            or row['captured_at'] > grouped[company]['lastEvidenceAt']
        ):
            grouped[company]['lastEvidenceAt'] = row['captured_at']

    for company, record in grouped.items():
        review = derive_auto_age_review(company, record['evidence'])
        record['ageRiskConclusion'] = review['conclusion']
        record['ageRiskConfidence'] = review['confidence']
        record['riskLevel'] = review['riskLevel']
        record['boycottRecommended'] = review['boycottRecommended']
        record['conclusionReason'] = review['reason']
        record['evidenceLevel'] = review['evidenceLevel']
        record['conclusionEvidenceCount'] = review['evidenceCount']
        record['conclusionProductName'] = review['productName']
        record['conclusionBusinessLine'] = review['businessLine']
        record['lastReviewedAt'] = review['reviewedAt']
        if review['lastEvidenceAt']:
            record['lastEvidenceAt'] = review['lastEvidenceAt']

    for company, suggestions in pending_products.items():
        if company not in grouped:
            product_line_list = product_lines.get(company, [])
            summary = company_summaries.get(company, {})
            review = derive_auto_age_review(company, [])
            grouped[company] = {
                'id': _stable_company_id(company),
                'name': company,
                'industry': '待补充',
                'riskLevel': summary.get('riskLevel') or 'medium',
                'verificationStatus': 'pending',
                'boycottRecommended': summary.get('boycottRecommended', False),
                'lastUpdated': '',
                'lastEvidenceAt': summary.get('lastEvidenceAt') or '',
                'lastReviewedAt': summary.get('lastReviewedAt') or review['reviewedAt'],
                'summary': '来自数据库导出，需持续补充多源证据。',
                'products': approved_products.get(company, []),
                'productLines': product_line_list,
                'productLine': summary.get('productLine') or (product_line_list[0]['businessLine'] if product_line_list else ''),
                'ageRiskConclusion': review['conclusion'],
                'ageRiskConfidence': review['confidence'],
                'conclusionReason': review['reason'],
                'evidenceLevel': review['evidenceLevel'],
                'conclusionProductName': review['productName'],
                'conclusionBusinessLine': review['businessLine'],
                'conclusionEvidenceCount': review['evidenceCount'],
                'pendingProducts': suggestions,
                'evidence': [],
            }
        else:
            grouped[company]['pendingProducts'] = suggestions

    result = list(grouped.values())
    result.sort(key=lambda x: (x['verificationStatus'], x['name']))
    return result
