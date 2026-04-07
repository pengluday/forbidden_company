#!/usr/bin/env python3
"""Collect job descriptions through Firecrawl and store them in SQLite/CSV."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from collectors.collection_utils import (
    append_csv,
    build_collect_row,
    ensure_sqlite_schema,
    extract_fields,
    is_large_company,
    normalize_dedupe_key,
    read_url_list,
    today_iso,
    write_csv,
)
from collectors.firecrawl_client import FirecrawlError, scrape_url


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect job descriptions through Firecrawl.")
    p.add_argument("--input-urls", required=True, help="Text file of URLs to scrape.")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--merge-csv", default="", help="Optional CSV to append new rows into.")
    p.add_argument("--db", default="", help="Optional SQLite DB path for direct insertion.")
    p.add_argument("--collector", default="firecrawl", help="Collector identifier.")
    p.add_argument("--source-platform", default="猎聘", help="Source platform label.")
    p.add_argument("--api-key", default="", help="Optional Firecrawl API key override.")
    p.add_argument("--base-url", default="https://api.firecrawl.dev", help="Firecrawl API base URL.")
    p.add_argument("--skip-no-evidence", action="store_true", help="Skip rows without age quote.")
    p.add_argument("--limit", type=int, default=100, help="Maximum URLs to process from the input file.")
    return p.parse_args()


def load_existing_company_keys(conn: sqlite3.Connection, source_platform: str) -> set[str]:
    rows = conn.execute(
        '''
        SELECT company_name
        FROM collected_evidence
        ''',
    ).fetchall()
    return {normalize_dedupe_key(row[0]) for row in rows if row[0]}


def load_existing_urls_from_db(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute('SELECT source_url FROM collected_evidence').fetchall()
    return {row[0] for row in rows if row[0]}


def insert_rows_into_db(db_path: Path, rows: list[dict[str, str]], source_platform: str) -> int:
    if not rows:
        return 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    inserted = 0
    try:
        existing_company_keys = load_existing_company_keys(conn, source_platform)
        existing_urls = load_existing_urls_from_db(conn)

        for row in rows:
            company_key = normalize_dedupe_key(row["company_name"])
            if company_key in existing_company_keys or row["source_url"] in existing_urls:
                print(f"[SKIP] Duplicate company/platform: {row['company_name']} | {row['source_platform']}")
                continue

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
            existing_company_keys.add(company_key)
            existing_urls.add(row["source_url"])
            inserted += 1

        conn.commit()
    finally:
        conn.close()

    return inserted


def parsed_payload_to_text(data: dict) -> str:
    html = (data.get("html") or "").strip()
    markdown = (data.get("markdown") or "").strip()
    return html or markdown or ""


def main() -> int:
    args = parse_args()
    input_urls = Path(args.input_urls)
    output_csv = Path(args.output_csv)
    merge_csv = Path(args.merge_csv) if args.merge_csv else None
    db_path = Path(args.db) if args.db else None

    if not input_urls.exists():
        print(f"[ERROR] URL list not found: {input_urls}", file=sys.stderr)
        return 1

    urls = read_url_list(input_urls)[: args.limit]
    if not urls:
        print("[WARN] No URLs to collect.")
        write_csv(output_csv, [])
        return 0

    if db_path:
        ensure_sqlite_schema(db_path)

    existing_urls = set()
    existing_company_keys: set[str] = set()
    if merge_csv and merge_csv.exists():
        from collectors.collection_utils import load_existing_urls

        existing_urls = load_existing_urls(merge_csv)
    if db_path and db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            existing_company_keys = load_existing_company_keys(conn, args.source_platform)
            existing_urls |= load_existing_urls_from_db(conn)
        finally:
            conn.close()

    captured_at = today_iso()
    rows: list[dict[str, str]] = []
    seen_run_keys: set[str] = set()
    seen_run_urls: set[str] = set()

    for idx, url in enumerate(urls, start=1):
        if url in existing_urls or url in seen_run_urls:
            print(f"[SKIP] Duplicate URL: {url}")
            continue

        try:
            payload = scrape_url(url, api_key=args.api_key, base_url=args.base_url, timeout=60)
        except FirecrawlError as exc:
            print(f"[WARN] Firecrawl scrape failed: {url} ({exc})")
            continue

        data = payload.get("data") if isinstance(payload, dict) else {}
        data = data if isinstance(data, dict) else {}
        text = parsed_payload_to_text(data)
        parsed = extract_fields(text, url)

        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        if metadata:
            meta_title = str(metadata.get("title") or "")
            parsed["job_title"] = parsed["job_title"] or meta_title
            parsed["source_title"] = parsed["source_title"] or meta_title
            if not parsed["company_name"] and meta_title:
                parsed["company_name"] = meta_title.split("招聘")[0].strip("-_ /")

        if parsed.get("company_scale") and not is_large_company(parsed["company_scale"]):
            print(f"[SKIP] Small/unknown company scale: {parsed['company_name']} | {parsed['company_scale']}")
            continue

        if args.skip_no_evidence and not parsed["evidence_quote"]:
            print(f"[SKIP] No evidence quote: {url}")
            continue

        row = build_collect_row(
            parsed=parsed,
            captured_at=captured_at,
            collector=args.collector,
            source_platform=args.source_platform,
            index=idx,
            record_prefix="fc",
        )

        company_key = normalize_dedupe_key(row["company_name"])
        if company_key in seen_run_keys or company_key in existing_company_keys:
            print(f"[SKIP] Duplicate company in this run: {row['company_name']}")
            continue

        rows.append(row)
        seen_run_keys.add(company_key)
        seen_run_urls.add(url)
        print(f"[OK] {url}")

    write_csv(output_csv, rows)
    print(f"[DONE] Wrote {len(rows)} rows to {output_csv}")

    if merge_csv:
        appended = append_csv(merge_csv, rows)
        print(f"[DONE] Appended {appended} rows to {merge_csv}")

    if db_path:
        inserted = insert_rows_into_db(db_path, rows, args.source_platform)
        print(f"[DONE] Inserted {inserted} rows into {db_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
