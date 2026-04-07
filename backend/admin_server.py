#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from csv import DictReader
import json
import re
import sqlite3
import os
import subprocess
import shutil
import tempfile
import uuid
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from collectors.collection_utils import normalize_company_name, normalize_dedupe_key
from backend.company_export import build_company_records, derive_auto_age_review, sync_company_summaries

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'data' / 'forbidden_company.db'
SCHEMA_PATH = ROOT / 'db' / 'schema.sql'
XHS_COOKIE_PATH = ROOT / 'data' / 'xiaohongshu-cookie.txt'
ZHILIAN_COOKIE_PATH = ROOT / 'data' / 'zhaopin-cookie.txt'
XHS_PLUGIN_ARTIFACTS_DIR = ROOT / 'data' / 'xhs-plugin-results'


def ensure_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not SCHEMA_PATH.exists():
        raise RuntimeError(f'Schema not found: {SCHEMA_PATH}')

    schema_sql = SCHEMA_PATH.read_text(encoding='utf-8')
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_date() -> str:
    return dt.date.today().isoformat()


def make_record_id() -> str:
    return f"manual-{dt.date.today().strftime('%Y%m%d')}-{uuid.uuid4().hex[:8]}"


def has_collected_company_duplicate(conn: sqlite3.Connection, company_name: str, source_platform: str) -> bool:
    target = normalize_dedupe_key(company_name)
    rows = conn.execute(
        '''
        SELECT company_name
        FROM collected_evidence
        ''',
    ).fetchall()
    for row in rows:
        if normalize_dedupe_key(row['company_name']) == target:
            return True
    return False


def has_product_duplicate(conn: sqlite3.Connection, company_name: str, product_name: str) -> bool:
    target_company = normalize_company_name(company_name)
    target_product = normalize_company_name(product_name)

    approved = conn.execute(
        'SELECT company_name, product_name FROM company_products WHERE company_name = ? OR product_name = ?',
        (company_name, product_name),
    ).fetchall()
    for row in approved:
        if normalize_company_name(row['company_name']) == target_company and normalize_company_name(row['product_name']) == target_product:
            return True

    pending = conn.execute(
        'SELECT company_name, product_name FROM pending_product_submissions WHERE company_name = ? OR product_name = ?',
        (company_name, product_name),
    ).fetchall()
    for row in pending:
        if normalize_company_name(row['company_name']) == target_company and normalize_company_name(row['product_name']) == target_product:
            return True
    return False


def json_response(handler: SimpleHTTPRequestHandler, payload: dict, status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json; charset=utf-8')
    handler.send_header('Access-Control-Allow-Origin', '*')
    handler.send_header('Access-Control-Allow-Headers', 'Content-Type')
    handler.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    handler.send_header('Content-Length', str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def read_xhs_cookie() -> str:
    try:
        return XHS_COOKIE_PATH.read_text(encoding='utf-8').strip()
    except OSError:
        return ''


def write_xhs_cookie(cookie: str) -> None:
    XHS_COOKIE_PATH.parent.mkdir(parents=True, exist_ok=True)
    XHS_COOKIE_PATH.write_text(cookie.strip() + '\n', encoding='utf-8')
    try:
        os.chmod(XHS_COOKIE_PATH, 0o600)
    except OSError:
        pass


def xhs_cookie_status() -> dict:
    cookie = read_xhs_cookie()
    return {
        'ok': True,
        'configured': bool(cookie),
        'length': len(cookie),
        'path': str(XHS_COOKIE_PATH),
    }


def xhs_plugin_status() -> dict:
    status = xhs_cookie_status()
    return {
        'ok': True,
        'service': {
            'name': 'forbidden-company-admin',
            'version': '1',
            'online': True,
        },
        'xhs': {
            'cookie_configured': status['configured'],
            'cookie_length': status['length'],
            'cookie_path': status['path'],
        },
        'defaults': {
            'comment_limit': 0,
            'include_comments': True,
        },
        'endpoints': {
            'collect': '/api/xhs-plugin/collect',
            'refresh': '/api/xhs-plugin/refresh',
            'status': '/api/xhs-plugin/status',
            'cookie': '/api/xhs-cookie',
        },
    }


def update_xhs_cookie(payload: dict) -> dict:
    cookie = (payload.get('cookie') or '').strip()
    if not cookie:
        raise ValueError('cookie is required')
    write_xhs_cookie(cookie)
    return xhs_cookie_status()


def read_zhilian_cookie_file(cookie_file: str) -> str:
    path = Path(cookie_file).expanduser() if cookie_file else ZHILIAN_COOKIE_PATH
    try:
        return path.read_text(encoding='utf-8').strip()
    except OSError:
        return ''


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = DictReader(f)
        return [dict(row) for row in reader]


def _xhs_result_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    comment_count = sum(1 for row in rows if '#comment-' in (row.get('source_url') or ''))
    post_count = max(0, len(rows) - comment_count)
    return {
        'post_count': post_count,
        'comment_count': comment_count,
        'total_count': len(rows),
    }


def _serve_url_for_path(path: str | None) -> str:
    if not path:
        return ''
    try:
        resolved = Path(path).resolve()
        rel = resolved.relative_to(ROOT)
    except Exception:  # noqa: BLE001
        return ''
    return '/' + rel.as_posix()


def _enrich_xhs_result(result: dict) -> dict:
    output_csv = (result.get('output_csv') or '').strip()
    rows = _read_csv_rows(Path(output_csv)) if output_csv else []
    counts = _xhs_result_counts(rows)
    preview_rows = rows[:20]
    artifact_id = f"xhs-{dt.datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    artifact_dir = XHS_PLUGIN_ARTIFACTS_DIR / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    csv_path = artifact_dir / 'result.csv'
    json_path = artifact_dir / 'result.json'
    if output_csv:
        try:
            shutil.copyfile(output_csv, csv_path)
        except OSError:
            csv_path = Path(output_csv)
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    enriched = dict(result)
    enriched.update(counts)
    enriched['record_count'] = counts['total_count']
    enriched['preview_rows'] = preview_rows
    enriched['artifact_id'] = artifact_id
    enriched['download_csv_path'] = str(csv_path)
    enriched['download_json_path'] = str(json_path)
    enriched['download_csv_url'] = _serve_url_for_path(str(csv_path))
    enriched['download_json_url'] = _serve_url_for_path(str(json_path))
    return enriched


def _with_absolute_download_urls(result: dict, base_url: str) -> dict:
    enriched = dict(result)
    base = base_url.rstrip('/')
    for key in ('download_csv_path', 'download_json_path'):
        path_value = (result.get(key) or '').strip()
        if not path_value:
            continue
        url_path = _serve_url_for_path(path_value)
        if url_path:
            enriched[key.replace('_path', '_url')] = base + url_path
    return enriched


def read_json(handler: SimpleHTTPRequestHandler) -> dict:
    length = int(handler.headers.get('Content-Length', '0'))
    raw = handler.rfile.read(length) if length > 0 else b'{}'
    if not raw:
        return {}
    return json.loads(raw.decode('utf-8'))


def list_collected(params: dict[str, list[str]]) -> dict:
    status = (params.get('status') or ['all'])[0]
    query = (params.get('q') or [''])[0].strip()

    where = []
    args: list[str] = []
    if status and status != 'all':
        where.append('verification_status = ?')
        args.append(status)
    if query:
        where.append('(company_name LIKE ? OR source_title LIKE ? OR source_url LIKE ?)')
        like = f'%{query}%'
        args.extend([like, like, like])

    sql = '''
      SELECT
        record_id, company_name, source_platform, source_url, source_title,
        captured_at, verification_status, risk_level, boycott_recommended,
        evidence_quote, evidence_summary, city, job_title, notes
      FROM collected_evidence
    '''
    if where:
        sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY captured_at DESC, record_id DESC LIMIT 500'

    conn = connect_db()
    try:
        rows = [dict(r) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()
    return {'items': rows}


def create_collected(payload: dict) -> dict:
    company_name = (payload.get('company_name') or '').strip()
    source_url = (payload.get('source_url') or '').strip()
    if not company_name:
        raise ValueError('company_name is required')
    if not source_url:
        raise ValueError('source_url is required')

    record_id = (payload.get('record_id') or '').strip() or make_record_id()
    source_title = (payload.get('source_title') or '').strip() or '招聘线索'

    row = {
        'record_id': record_id,
        'company_name': company_name,
        'uscc_or_entity_id': (payload.get('uscc_or_entity_id') or '').strip(),
        'source_type': (payload.get('source_type') or 'jobsite').strip() or 'jobsite',
        'source_platform': (payload.get('source_platform') or '智联招聘').strip() or '智联招聘',
        'source_url': source_url,
        'source_title': source_title,
        'published_at': (payload.get('published_at') or '').strip(),
        'captured_at': (payload.get('captured_at') or now_date()).strip() or now_date(),
        'city': (payload.get('city') or '').strip(),
        'job_title': (payload.get('job_title') or '').strip(),
        'evidence_quote': (payload.get('evidence_quote') or '').strip(),
        'evidence_summary': (payload.get('evidence_summary') or '').strip(),
        'screenshot_path': (payload.get('screenshot_path') or '').strip(),
        'collector': (payload.get('collector') or 'admin-ui').strip(),
        'verification_status': (payload.get('verification_status') or 'pending').strip() or 'pending',
        'risk_level': (payload.get('risk_level') or 'medium').strip() or 'medium',
        'boycott_recommended': 1 if payload.get('boycott_recommended') else 0,
        'notes': (payload.get('notes') or '').strip(),
    }

    conn = connect_db()
    try:
        if has_collected_company_duplicate(conn, company_name, row['source_platform']):
            raise ValueError('同公司同平台已存在，已自动去重')
        conn.execute(
            '''
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
            ''',
            row,
        )
        refresh_company_summary(conn, company_name)
        conn.commit()
    finally:
        conn.close()

    return {'ok': True, 'record_id': record_id}


def verify_record(payload: dict) -> dict:
    record_id = (payload.get('record_id') or '').strip()
    verifier = (payload.get('verifier') or '').strip()
    status = (payload.get('status') or 'partial').strip()
    risk_level = (payload.get('risk_level') or 'medium').strip()
    note = (payload.get('note') or '').strip()
    boycott = 1 if payload.get('boycott_recommended') else 0

    if not record_id:
        raise ValueError('record_id is required')
    if not verifier:
        raise ValueError('verifier is required')
    if status not in {'partial', 'verified', 'error'}:
        raise ValueError('status must be partial, verified, or error')
    if risk_level not in {'low', 'medium', 'high'}:
        raise ValueError('risk_level must be low/medium/high')

    conn = connect_db()
    try:
        row = conn.execute(
            'SELECT id, record_id, company_name FROM collected_evidence WHERE record_id = ?',
            (record_id,),
        ).fetchone()
        if row is None:
            raise ValueError('record_id not found')

        conn.execute(
            '''
            INSERT INTO verified_evidence (
              collected_id, record_id, company_name, verification_status,
              risk_level, boycott_recommended, verifier, verification_note, verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_id) DO UPDATE SET
              company_name=excluded.company_name,
              verification_status=excluded.verification_status,
              risk_level=excluded.risk_level,
              boycott_recommended=excluded.boycott_recommended,
              verifier=excluded.verifier,
              verification_note=excluded.verification_note,
              verified_at=excluded.verified_at
            ''',
            (row['id'], row['record_id'], row['company_name'], status, risk_level, boycott, verifier, note, now_date()),
        )
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
            (status, risk_level, boycott, note, note, record_id),
        )
        refresh_company_summary(conn, row['company_name'])
        conn.commit()
    finally:
        conn.close()

    return {'ok': True, 'record_id': record_id, 'status': status}


def upsert_product(payload: dict) -> dict:
    company = (payload.get('company_name') or '').strip()
    product = (payload.get('product_name') or '').strip()
    if not company:
        raise ValueError('company_name is required')
    if not product:
        raise ValueError('product_name is required')

    category = (payload.get('product_category') or '').strip()
    url = (payload.get('product_url') or '').strip()
    confidence = (payload.get('confidence') or 'unverified').strip()
    note = (payload.get('source_note') or 'admin-ui').strip()

    if confidence not in {'unverified', 'partial', 'verified'}:
        raise ValueError('confidence must be unverified/partial/verified')

    conn = connect_db()
    try:
        if has_product_duplicate(conn, company, product):
            raise ValueError('相同公司和产品已存在，已自动去重')
        conn.execute(
            '''
            INSERT INTO pending_product_submissions (
              company_name, product_name, product_category, product_url, source_note, submitted_by, review_status
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending')
            ''',
            (company, product, category, url, note, payload.get('submitted_by') or 'admin-ui'),
        )
        refresh_company_summary(conn, company)
        conn.commit()
    finally:
        conn.close()

    return {'ok': True, 'company_name': company, 'product_name': product, 'status': 'pending'}


def approve_product_submission(payload: dict) -> dict:
    submission_id = payload.get('submission_id')
    reviewer = (payload.get('reviewer') or '').strip()
    note = (payload.get('reviewed_note') or '').strip()
    if not submission_id:
        raise ValueError('submission_id is required')
    if not reviewer:
        raise ValueError('reviewer is required')

    conn = connect_db()
    try:
        row = conn.execute(
            '''
            SELECT id, company_name, product_name, product_category, product_url, source_note
            FROM pending_product_submissions
            WHERE id = ? AND review_status = 'pending'
            ''',
            (submission_id,),
        ).fetchone()
        if row is None:
            raise ValueError('submission not found')

        conn.execute(
            '''
            INSERT INTO company_products (
              company_name, product_name, product_category, product_url, confidence, source_note
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_name, product_name) DO UPDATE SET
              product_category=excluded.product_category,
              product_url=excluded.product_url,
              confidence=excluded.confidence,
              source_note=excluded.source_note,
              updated_at=CURRENT_TIMESTAMP
            ''',
            (row['company_name'], row['product_name'], row['product_category'], row['product_url'], 'verified', row['source_note'] or note),
        )
        refresh_company_summary(conn, row['company_name'])
        conn.execute(
            '''
            UPDATE pending_product_submissions
            SET review_status = 'approved',
                reviewed_by = ?,
                reviewed_note = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            ''',
            (reviewer, note, submission_id),
        )
        conn.commit()
    finally:
        conn.close()

    return {'ok': True, 'submission_id': submission_id, 'status': 'approved'}


def _latest_company_product_line(conn: sqlite3.Connection, company: str) -> str:
    row = conn.execute(
        '''
        SELECT business_line
        FROM company_product_lines
        WHERE company_name = ?
        ORDER BY
          CASE confidence
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0
          END DESC,
          updated_at DESC,
          id DESC
        LIMIT 1
        ''',
        (company,),
    ).fetchone()
    if row and row['business_line']:
        return row['business_line']

    row = conn.execute(
        '''
        SELECT product_category
        FROM company_products
        WHERE company_name = ?
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        ''',
        (company,),
    ).fetchone()
    return (row['product_category'] if row and row['product_category'] else '') or ''


def refresh_company_summary(conn: sqlite3.Connection, company_name: str) -> None:
    evidence_rows = conn.execute(
        '''
        SELECT
          c.captured_at,
          c.source_title,
          c.job_title,
          c.evidence_quote,
          c.evidence_summary,
          c.notes,
          c.risk_level,
          c.boycott_recommended
        FROM collected_evidence c
        WHERE c.company_name = ?
          AND c.verification_status != 'error'
        ORDER BY c.captured_at DESC, c.id DESC
        ''',
        (company_name,),
    ).fetchall()
    evidence_count_row = conn.execute(
        'SELECT COUNT(*) AS count FROM collected_evidence WHERE company_name = ?',
        (company_name,),
    ).fetchone()

    review = derive_auto_age_review(company_name, evidence_rows)
    product_line = _latest_company_product_line(conn, company_name)
    age_risk_conclusion = review['conclusion']
    age_risk_confidence = review['confidence']
    conclusion_reason = review['reason']
    evidence_level = review['evidenceLevel']
    last_reviewed_at = review['reviewedAt']
    risk_level = review['riskLevel']
    boycott_recommended = int(review['boycottRecommended'])
    last_evidence_at = review['lastEvidenceAt'] or None
    evidence_count = int(evidence_count_row['count'] if evidence_count_row else 0)

    conn.execute(
        '''
        DELETE FROM company_conclusions
        WHERE company_name = ? AND reviewer = 'auto-engine'
        ''',
        (company_name,),
    )
    conn.execute(
        '''
        INSERT INTO company_conclusions (
          company_name, product_name, business_line, age_risk_conclusion,
          age_risk_confidence, reason, evidence_count, evidence_level,
          reviewer, reviewed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            company_name,
            review['productName'],
            review['businessLine'],
            age_risk_conclusion,
            age_risk_confidence,
            conclusion_reason,
            review['evidenceCount'] or evidence_count,
            evidence_level,
            'auto-engine',
            last_reviewed_at,
        ),
    )

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


def upsert_company_conclusion(payload: dict) -> dict:
    company = (payload.get('company_name') or '').strip()
    if not company:
        raise ValueError('company_name is required')

    conclusion = (payload.get('age_risk_conclusion') or 'insufficient').strip()
    confidence = (payload.get('age_risk_confidence') or 'low').strip()
    product_name = (payload.get('product_name') or '').strip()
    business_line = (payload.get('business_line') or '').strip()
    reason = (payload.get('reason') or '').strip()
    evidence_level = (payload.get('evidence_level') or 'L1').strip()
    reviewer = (payload.get('reviewer') or 'admin-ui').strip()
    reviewed_at = (payload.get('reviewed_at') or now_date()).strip() or now_date()
    evidence_count = int(payload.get('evidence_count') or 0)

    if conclusion not in {'clear', 'suspected', 'insufficient', 'none'}:
        raise ValueError('age_risk_conclusion must be clear/suspected/insufficient/none')
    if confidence not in {'low', 'medium', 'high'}:
        raise ValueError('age_risk_confidence must be low/medium/high')
    if evidence_level not in {'L1', 'L2', 'L3'}:
        raise ValueError('evidence_level must be L1/L2/L3')

    conn = connect_db()
    try:
        conn.execute(
            '''
            INSERT INTO company_conclusions (
              company_name, product_name, business_line, age_risk_conclusion,
              age_risk_confidence, reason, evidence_count, evidence_level,
              reviewer, reviewed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (company, product_name, business_line, conclusion, confidence, reason, evidence_count, evidence_level, reviewer, reviewed_at),
        )
        refresh_company_summary(conn, company)
        conn.commit()
    finally:
        conn.close()

    return {'ok': True, 'company_name': company, 'age_risk_conclusion': conclusion}


def upsert_company_product_line(payload: dict) -> dict:
    company = (payload.get('company_name') or '').strip()
    product = (payload.get('product_name') or '').strip()
    if not company:
        raise ValueError('company_name is required')
    if not product:
        raise ValueError('product_name is required')

    business_line = (payload.get('business_line') or '').strip()
    category = (payload.get('product_category') or '').strip()
    mapping_status = (payload.get('mapping_status') or 'unverified').strip()
    mapping_source = (payload.get('mapping_source') or 'admin-ui').strip()
    confidence = (payload.get('confidence') or 'low').strip()

    if mapping_status not in {'unverified', 'partial', 'verified'}:
        raise ValueError('mapping_status must be unverified/partial/verified')
    if confidence not in {'low', 'medium', 'high'}:
        raise ValueError('confidence must be low/medium/high')

    conn = connect_db()
    try:
        conn.execute(
            '''
            INSERT INTO company_product_lines (
              company_name, product_name, business_line, product_category,
              mapping_status, mapping_source, confidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_name, product_name) DO UPDATE SET
              business_line=excluded.business_line,
              product_category=excluded.product_category,
              mapping_status=excluded.mapping_status,
              mapping_source=excluded.mapping_source,
              confidence=excluded.confidence,
              updated_at=CURRENT_TIMESTAMP
            ''',
            (company, product, business_line, category, mapping_status, mapping_source, confidence),
        )
        refresh_company_summary(conn, company)
        conn.commit()
    finally:
        conn.close()

    return {'ok': True, 'company_name': company, 'product_name': product, 'mapping_status': mapping_status}


def list_products(params: dict[str, list[str]]) -> dict:
    query = (params.get('q') or [''])[0].strip()
    where = ''
    args: list[str] = []
    if query:
        where = 'WHERE company_name LIKE ? OR product_name LIKE ? OR product_category LIKE ?'
        like = f'%{query}%'
        args = [like, like, like]

    conn = connect_db()
    try:
        rows = conn.execute(
            f'''
            SELECT company_name, product_name, product_category, product_url, confidence, source_note, updated_at
            FROM company_products
            {where}
            ORDER BY updated_at DESC, company_name ASC
            LIMIT 500
            ''',
            args,
        ).fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()
    return {'items': items}


def list_company_product_lines(params: dict[str, list[str]]) -> dict:
    query = (params.get('q') or [''])[0].strip()
    where = ''
    args: list[str] = []
    if query:
        where = 'WHERE company_name LIKE ? OR product_name LIKE ? OR business_line LIKE ? OR product_category LIKE ?'
        like = f'%{query}%'
        args = [like, like, like, like]

    conn = connect_db()
    try:
        rows = conn.execute(
            f'''
            SELECT company_name, product_name, business_line, product_category, mapping_status, mapping_source, confidence, updated_at
            FROM company_product_lines
            {where}
            ORDER BY updated_at DESC, company_name ASC
            LIMIT 500
            ''',
            args,
        ).fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()
    return {'items': items}


def list_company_conclusions(params: dict[str, list[str]]) -> dict:
    query = (params.get('q') or [''])[0].strip()
    where = ''
    args: list[str] = []
    if query:
        where = 'WHERE company_name LIKE ? OR product_name LIKE ? OR business_line LIKE ? OR reason LIKE ?'
        like = f'%{query}%'
        args = [like, like, like, like]

    conn = connect_db()
    try:
        rows = conn.execute(
            f'''
            SELECT company_name, product_name, business_line, age_risk_conclusion,
                   age_risk_confidence, reason, evidence_count, evidence_level,
                   reviewer, reviewed_at, created_at
            FROM company_conclusions
            {where}
            ORDER BY created_at DESC, id DESC
            LIMIT 500
            ''',
            args,
        ).fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()
    return {'items': items}


def list_company_summaries(params: dict[str, list[str]]) -> dict:
    query = (params.get('q') or [''])[0].strip()
    where = ''
    args: list[str] = []
    if query:
        where = 'WHERE company_name LIKE ? OR product_line LIKE ? OR conclusion_reason LIKE ?'
        like = f'%{query}%'
        args = [like, like, like]

    conn = connect_db()
    try:
        rows = conn.execute(
            f'''
            SELECT company_name, product_line, age_risk_conclusion, age_risk_confidence,
                   risk_level, conclusion_reason, evidence_level, last_evidence_at,
                   last_reviewed_at, boycott_recommended, created_at, updated_at
            FROM companies
            {where}
            ORDER BY updated_at DESC, company_name ASC
            LIMIT 500
            ''',
            args,
        ).fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()
    return {'items': items}


def list_pending_product_submissions(params: dict[str, list[str]]) -> dict:
    query = (params.get('q') or [''])[0].strip()
    where = "WHERE review_status = 'pending'"
    args: list[str] = []
    if query:
        where += ' AND (company_name LIKE ? OR product_name LIKE ? OR product_category LIKE ?)'
        like = f'%{query}%'
        args = [like, like, like]

    conn = connect_db()
    try:
        rows = conn.execute(
            f'''
            SELECT id, company_name, product_name, product_category, product_url, source_note, submitted_by, review_status, created_at, updated_at
            FROM pending_product_submissions
            {where}
            ORDER BY created_at DESC, id DESC
            ''',
            args,
        ).fetchall()
        items = [dict(r) for r in rows]
    finally:
        conn.close()
    return {'items': items}


def run_zhilian_collection(options: dict | None = None) -> dict:
    options = options or {}
    today = dt.date.today().isoformat()
    output_csv = ROOT / 'data' / f'source-intake-zhilian-{today}.csv'
    cookie = (options.get('cookie') or '').strip()
    cookie_file = (options.get('cookie_file') or '').strip()
    if not cookie and not cookie_file and ZHILIAN_COOKIE_PATH.exists():
        cookie_file = str(ZHILIAN_COOKIE_PATH)
    cmd = [
        'python3',
        '-m',
        'collectors.collect_zhilian',
        '--output-csv',
        str(output_csv),
        '--merge-csv',
        str(ROOT / 'data' / 'source-intake-round1-jobsites.csv'),
        '--db',
        str(DB_PATH),
        '--collector',
        'backend-run-zhilian',
        '--limit',
        str(int(options.get('limit') or 100)),
        '--seed-url',
        'https://fe-api.zhaopin.com/c/i/search/positions?MmEwMD=5Hcl4QR9z3G1Ec3KpAuMe7xmQB0Nb91R_znn2UN5te.5JCi9zvtFJib1jJOx6GdQpA.xmwqTdnVzA9GykOQOsekA6xofO6RuXlYbMU9p24IBuVmhKrjcsQm9PU0ZB.NTS.Zm50JRcmhrlTxue6SrDUNzkkLQ40FQtIUjJVZW74OjC1dss6l_aJaZmGFHR1U1XdEFJNcQYTgf7ShSstmvtbp9RzfT1TBpskEXmWkW0sPbUxkc_gF_gSQ81JvL7qNyUElWkeFV962rko1YRP9YpLjluBn148Rzn5T4ykd0vDQvMgeMRmBWttbDbSRSulCWqyrqulMWf6_U2CaQ6ppD1WRg7nFzQ_12RD33if1M6SmJNXvMGSXK2OmKgYRkGa.lofRce7v7LrQJqZjEy1wsPFa&c1K5tw0w6_=49Tir6qGM8dhvPt7R7LNHKMllt2oSy6EFj0wydkWJ.FWYR4dbjFwPEg_Ee2WyEFZhXz0HcDWPRPUVLmazjJr4vwu5cfXksOVzTMzztZUNpRbl7zPkZWn2JPlfwkejIlrDskntg87.o3wT20LhhAbiEetaAGGQ5885izXKd3.7bALLW_IHWUyL1AiZ2p5OxftbeN.JPI0tnTBujxFgP28gpujG6yfib7.jXr6xxRl_mSdpZBVMSLKVPAr1OUjsXozApp....CjrEyYVLcOFlCy3VK8EHCrxy2QT_A5DSKWDLU3HYzvuFxP54WapUIf5.jo6o7EL2mUg6XRQqjRicw_9zg2Z5bQnfTuYXmI356pv7OTHWeGhf76yr59euidb6ZtHaE1_SAQrvSwKUKlafg9.luowYpPaiTqLCD51pZA7UCJl4WJ2tSzQus4woUFczAdfYaAfSrJ3EQYqik127pJva',
    ]
    if cookie:
        cmd.extend(['--cookie', cookie])
    if cookie_file:
        cmd.extend(['--cookie-file', cookie_file])

    log_lines = [f"[COLLECT] start zhilian limit={cmd[cmd.index('--limit') + 1]}"]
    print(log_lines[0], flush=True)
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        cwd=str(ROOT),
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue
        log_lines.append(line)
        print(line, flush=True)
    result_code = proc.wait()
    if result_code != 0:
        fail_line = f"[COLLECT] failed zhilian rc={result_code}"
        log_lines.append(fail_line)
        print(fail_line, flush=True)
        raise RuntimeError('\n'.join(log_lines[-50:]) or 'collection failed')
    export_companies_json()
    done_line = f"[COLLECT] done zhilian rc={result_code} output={output_csv}"
    log_lines.append(done_line)
    print(done_line, flush=True)
    return {
        'ok': True,
        'output_csv': str(output_csv),
        'stdout': '',
        'stderr': '',
        'log': '\n'.join(log_lines),
    }
def run_firecrawl_collection(payload: dict | None = None) -> dict:
    payload = payload or {}
    urls = payload.get('urls') or []
    if isinstance(urls, str):
        urls = [line.strip() for line in urls.splitlines() if line.strip()]
    urls = [str(url).strip() for url in urls if str(url).strip()]
    if not urls:
        raise ValueError('urls is required')

    source_platform = (payload.get('source_platform') or '猎聘').strip() or '猎聘'
    collector = (payload.get('collector') or 'firecrawl-backend').strip() or 'firecrawl-backend'
    limit = int(payload.get('limit') or len(urls))
    skip_no_evidence = bool(payload.get('skip_no_evidence', True))
    api_key = (payload.get('api_key') or '').strip()
    base_url = (payload.get('base_url') or 'https://api.firecrawl.dev').strip() or 'https://api.firecrawl.dev'
    cookie = (payload.get('cookie') or '').strip()
    cookie_file = (payload.get('cookie_file') or '').strip()
    comment_api_host = (payload.get('comment_api_host') or 'https://edith.xiaohongshu.com').strip() or 'https://edith.xiaohongshu.com'

    today = dt.date.today().isoformat()
    output_csv = ROOT / 'data' / f'source-intake-firecrawl-{today}.csv'

    with tempfile.NamedTemporaryFile('w', encoding='utf-8', delete=False, suffix='.txt') as tmp:
        for url in urls[:limit]:
            tmp.write(url + '\n')
        temp_path = tmp.name

    cmd = [
        'python3',
        '-m',
        'collectors.collect_firecrawl',
        '--input-urls',
        temp_path,
        '--output-csv',
        str(output_csv),
        '--merge-csv',
        str(ROOT / 'data' / 'source-intake-round1-jobsites.csv'),
        '--db',
        str(DB_PATH),
        '--collector',
        collector,
        '--source-platform',
        source_platform,
        '--limit',
        str(limit),
        '--base-url',
        base_url,
    ]
    if api_key:
        cmd.extend(['--api-key', api_key])
    if skip_no_evidence:
        cmd.append('--skip-no-evidence')

    log_lines = [f"[COLLECT] start firecrawl limit={limit} platform={source_platform}"]
    print(log_lines[0], flush=True)
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        cwd=str(ROOT),
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue
        log_lines.append(line)
        print(line, flush=True)
    result_code = proc.wait()
    try:
        Path(temp_path).unlink(missing_ok=True)
    except OSError:
        pass
    if result_code != 0:
        fail_line = f"[COLLECT] failed firecrawl rc={result_code}"
        log_lines.append(fail_line)
        print(fail_line, flush=True)
        raise RuntimeError('\n'.join(log_lines[-50:]) or 'firecrawl collection failed')
    export_companies_json()
    done_line = f"[COLLECT] done firecrawl rc={result_code} output={output_csv}"
    log_lines.append(done_line)
    print(done_line, flush=True)
    return {
        'ok': True,
        'output_csv': str(output_csv),
        'stdout': '',
        'stderr': '',
        'log': '\n'.join(log_lines),
    }


def run_xiaohongshu_collection(payload: dict | None = None) -> dict:
    payload = payload or {}
    urls = payload.get('urls') or []
    if isinstance(urls, str):
        urls = [line.strip() for line in urls.splitlines() if line.strip()]
    urls = [str(url).strip() for url in urls if str(url).strip()]
    if not urls:
        raise ValueError('urls is required')

    collector = (payload.get('collector') or 'xiaohongshu-backend').strip() or 'xiaohongshu-backend'
    source_platform = (payload.get('source_platform') or '小红书').strip() or '小红书'
    company_name = (payload.get('company_name') or '').strip()
    limit = int(payload.get('limit') or len(urls))
    raw_comment_limit = payload.get('comment_limit')
    comment_limit = int(raw_comment_limit) if raw_comment_limit not in (None, '') else 3
    include_comments = bool(payload.get('include_comments', True))
    skip_no_evidence = bool(payload.get('skip_no_evidence', True))
    api_key = (payload.get('api_key') or '').strip()
    base_url = (payload.get('base_url') or 'https://api.firecrawl.dev').strip() or 'https://api.firecrawl.dev'
    cookie = (payload.get('cookie') or '').strip()
    cookie_file = (payload.get('cookie_file') or '').strip()
    comment_api_host = (payload.get('comment_api_host') or 'https://edith.xiaohongshu.com').strip() or 'https://edith.xiaohongshu.com'
    refresh_url = (payload.get('refresh_url') or '').strip()
    if not cookie and not cookie_file and XHS_COOKIE_PATH.exists():
        cookie_file = str(XHS_COOKIE_PATH)

    today = dt.date.today().isoformat()
    output_csv = ROOT / 'data' / f'source-intake-xiaohongshu-{today}.csv'

    with tempfile.NamedTemporaryFile('w', encoding='utf-8', delete=False, suffix='.txt') as tmp:
        for url in urls[:limit]:
            tmp.write(url + '\n')
        temp_path = tmp.name

    cmd = [
        'python3',
        '-m',
        'collectors.collect_xiaohongshu',
        '--input-urls',
        temp_path,
        '--output-csv',
        str(output_csv),
        '--merge-csv',
        str(ROOT / 'data' / 'source-intake-round1-jobsites.csv'),
        '--db',
        str(DB_PATH),
        '--collector',
        collector,
        '--source-platform',
        source_platform,
        '--limit',
        str(limit),
        '--comment-limit',
        str(comment_limit),
        '--base-url',
        base_url,
    ]
    if company_name:
        cmd.extend(['--company', company_name])
    if api_key:
        cmd.extend(['--api-key', api_key])
    if cookie:
        cmd.extend(['--cookie', cookie])
    if cookie_file:
        cmd.extend(['--cookie-file', cookie_file])
    if comment_api_host:
        cmd.extend(['--comment-api-host', comment_api_host])
    if refresh_url:
        cmd.extend(['--refresh-url', refresh_url])
    if include_comments:
        cmd.append('--include-comments')
    if skip_no_evidence:
        cmd.append('--skip-no-evidence')

    log_lines = [
        f"[COLLECT] start xiaohongshu limit={limit} comments={comment_limit} company={company_name or 'auto'}",
    ]
    print(log_lines[0], flush=True)
    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
        cwd=str(ROOT),
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue
        log_lines.append(line)
        print(line, flush=True)
    result_code = proc.wait()
    try:
        Path(temp_path).unlink(missing_ok=True)
    except OSError:
        pass
    if result_code != 0:
        fail_line = f"[COLLECT] failed xiaohongshu rc={result_code}"
        log_lines.append(fail_line)
        print(fail_line, flush=True)
        raise RuntimeError('\n'.join(log_lines[-50:]) or 'xiaohongshu collection failed')
    export_companies_json()
    done_line = f"[COLLECT] done xiaohongshu rc={result_code} output={output_csv}"
    log_lines.append(done_line)
    print(done_line, flush=True)
    return {
        'ok': True,
        'output_csv': str(output_csv),
        'stdout': '',
        'stderr': '',
        'log': '\n'.join(log_lines),
    }


def refresh_xiaohongshu_single(payload: dict | None = None) -> dict:
    payload = payload or {}
    url = (payload.get('url') or '').strip()
    if not url:
        raise ValueError('url is required')

    single_payload = dict(payload)
    single_payload['urls'] = [url]
    single_payload['limit'] = 1
    single_payload['refresh_url'] = url
    if 'comment_limit' not in single_payload:
        single_payload['comment_limit'] = 0
    result = run_xiaohongshu_collection(single_payload)
    result['refreshed_url'] = url
    return result


def build_company_list() -> list[dict]:
    conn = connect_db()
    try:
        return build_company_records(conn, include_pending=True)
    finally:
        conn.close()


def export_companies_json() -> dict:
    conn = connect_db()
    try:
        result = build_company_records(conn, include_pending=True)
        sync_company_summaries(conn, result)
        conn.commit()
    finally:
        conn.close()

    output_path = ROOT / 'data' / 'companies.json'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    return {'ok': True, 'output': str(output_path)}


class AdminHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def translate_path(self, path: str) -> str:
        # Force /admin to serve admin/index.html
        if path in {'/admin', '/admin/'}:
            return str(ROOT / 'admin' / 'index.html')
        return super().translate_path(path)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == '/api/collected':
            try:
                payload = list_collected(parse_qs(parsed.query))
                json_response(self, payload)
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/products':
            try:
                payload = list_products(parse_qs(parsed.query))
                json_response(self, payload)
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/company-summaries':
            try:
                payload = list_company_summaries(parse_qs(parsed.query))
                json_response(self, payload)
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/company-product-lines':
            try:
                payload = list_company_product_lines(parse_qs(parsed.query))
                json_response(self, payload)
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/company-conclusions':
            try:
                payload = list_company_conclusions(parse_qs(parsed.query))
                json_response(self, payload)
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/product-submissions':
            try:
                payload = list_pending_product_submissions(parse_qs(parsed.query))
                json_response(self, payload)
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/public/companies':
            try:
                json_response(self, {'items': build_company_list()})
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/collect-zhilian':
            try:
                json_response(self, run_zhilian_collection(read_json(self)))
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/collect-xiaohongshu':
            try:
                json_response(self, run_xiaohongshu_collection(read_json(self)))
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/refresh-xiaohongshu':
            try:
                json_response(self, refresh_xiaohongshu_single(read_json(self)))
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/xhs-cookie':
            try:
                json_response(self, xhs_cookie_status())
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return
        if parsed.path == '/api/xhs-plugin/status':
            try:
                json_response(self, xhs_plugin_status())
            except Exception as e:  # pylint: disable=broad-except
                json_response(self, {'error': str(e)}, status=500)
            return

        if parsed.path == '/api/health':
            json_response(self, {'ok': True, 'db': str(DB_PATH)})
            return

        super().do_GET()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            payload = read_json(self)
            if parsed.path == '/api/collect':
                result = create_collected(payload)
                export_companies_json()
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/verify':
                result = verify_record(payload)
                export_companies_json()
                json_response(self, result)
                return
            if parsed.path == '/api/collect-zhilian':
                result = run_zhilian_collection(payload)
                json_response(self, result)
                return
            if parsed.path == '/api/collect-xiaohongshu':
                result = run_xiaohongshu_collection(payload)
                json_response(self, result)
                return
            if parsed.path == '/api/refresh-xiaohongshu':
                result = refresh_xiaohongshu_single(payload)
                json_response(self, result)
                return
            if parsed.path == '/api/xhs-cookie':
                result = update_xhs_cookie(payload)
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/xhs-plugin/collect':
                result = _with_absolute_download_urls(
                    _enrich_xhs_result(run_xiaohongshu_collection(payload)),
                    f"http://{self.headers.get('Host') or '127.0.0.1:8787'}",
                )
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/xhs-plugin/refresh':
                result = _with_absolute_download_urls(
                    _enrich_xhs_result(refresh_xiaohongshu_single(payload)),
                    f"http://{self.headers.get('Host') or '127.0.0.1:8787'}",
                )
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/product':
                result = upsert_product(payload)
                export_companies_json()
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/company-conclusion':
                result = upsert_company_conclusion(payload)
                export_companies_json()
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/company-product-line':
                result = upsert_company_product_line(payload)
                export_companies_json()
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/approve-product':
                result = approve_product_submission(payload)
                export_companies_json()
                json_response(self, result)
                return
            if parsed.path == '/api/public/collect':
                payload.setdefault('collector', 'community-user')
                payload.setdefault('verification_status', 'pending')
                payload.setdefault('risk_level', 'medium')
                payload.setdefault('source_type', 'jobsite')
                result = create_collected(payload)
                export_companies_json()
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/public/product-suggestion':
                payload.setdefault('submitted_by', 'community-user')
                payload.setdefault('source_note', 'community-user')
                result = upsert_product(payload)
                export_companies_json()
                json_response(self, result, status=201)
                return
            if parsed.path == '/api/export':
                json_response(self, export_companies_json())
                return

            json_response(self, {'error': 'Not found'}, status=404)
        except ValueError as e:
            json_response(self, {'error': str(e)}, status=400)
        except sqlite3.IntegrityError as e:
            msg = str(e)
            if re.search(r'UNIQUE constraint failed', msg):
                json_response(self, {'error': '重复记录：record_id 或 source_url 已存在'}, status=409)
            else:
                json_response(self, {'error': msg}, status=409)
        except Exception as e:  # pylint: disable=broad-except
            json_response(self, {'error': str(e)}, status=500)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Run admin web server for collection and verification.')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8787)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    ensure_db()
    export_companies_json()

    server = ThreadingHTTPServer((args.host, args.port), AdminHandler)
    print(f'[START] admin server at http://{args.host}:{args.port}/admin/')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

    print('[STOP] admin server stopped')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
