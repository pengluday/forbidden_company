#!/usr/bin/env python3
"""Collect recent Liepin job evidence directly from the live search feed."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from collectors.collection_utils import (
    append_csv,
    DEFAULT_MAJOR_CITIES,
    build_collect_row,
    ensure_sqlite_schema,
    extract_fields,
    extract_job_url,
    fetch_html,
    fetch_liepin_search_cards,
    is_large_company,
    normalize_dedupe_key,
    today_iso,
    write_csv,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect recent Liepin job evidence.")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--merge-csv", default="", help="Optional CSV to append new rows into.")
    p.add_argument("--db", default="", help="Optional SQLite DB path for direct insertion.")
    p.add_argument("--collector", default="script", help="Collector identifier.")
    p.add_argument("--default-risk", default="medium", choices=["low", "medium", "high"])
    p.add_argument("--skip-no-evidence", action="store_true", help="Skip rows without age quote.")
    p.add_argument("--limit", type=int, default=100, help="Maximum number of recent jobs to inspect.")
    p.add_argument("--page-size", type=int, default=40, help="Search page size when crawling Liepin.")
    p.add_argument("--key", default="", help="Optional search keyword for Liepin.")
    p.add_argument("--city", default="", help="Optional city code for Liepin search.")
    p.add_argument(
        "--cities",
        default="",
        help="Comma-separated city list. Defaults to Hangzhou if omitted.",
    )
    p.add_argument("--dq", default="", help="Optional location filter for Liepin search.")
    p.add_argument("--pub-time", default="", help="Optional publish time filter for Liepin search.")
    return p.parse_args()


def load_existing_company_keys(conn: sqlite3.Connection, source_platform: str) -> set[str]:
    rows = conn.execute(
        '''
        SELECT company_name
        FROM collected_evidence
        ''',
    ).fetchall()
    return {normalize_dedupe_key(row[0], source_platform) for row in rows if row[0]}


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
            company_key = normalize_dedupe_key(row["company_name"], row["source_platform"])
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


def _card_dict(card: object) -> dict:
    return card if isinstance(card, dict) else {}


def _first_text(*values: object) -> str:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def card_company_name(card: dict) -> str:
    comp = _card_dict(card.get("comp"))
    return _first_text(
        comp.get("compName"),
        comp.get("companyName"),
        comp.get("name"),
        card.get("compName"),
        card.get("companyName"),
        card.get("name"),
    )


def card_job_title(card: dict) -> str:
    job = _card_dict(card.get("job"))
    return _first_text(
        job.get("title"),
        job.get("jobTitle"),
        job.get("positionName"),
        card.get("jobTitle"),
        card.get("title"),
        card.get("positionName"),
    )


def card_city(card: dict) -> str:
    job = _card_dict(card.get("job"))
    return _first_text(
        job.get("dq"),
        job.get("city"),
        job.get("workCity"),
        card.get("dq"),
        card.get("city"),
    )


def card_source_title(card: dict) -> str:
    title = card_job_title(card)
    if title:
        return f"{title}招聘（猎聘）"
    return "职位招聘（猎聘）"


def enrich_from_card(parsed: dict[str, str], card: dict) -> dict[str, str]:
    merged = dict(parsed)
    merged["company_name"] = merged.get("company_name") or card_company_name(card)
    merged["job_title"] = merged.get("job_title") or card_job_title(card)
    merged["city"] = merged.get("city") or card_city(card)
    merged["source_title"] = merged.get("source_title") or card_source_title(card)
    company_scale = _first_text(
        merged.get("company_scale"),
        _card_dict(card.get("comp")).get("compScale"),
        _card_dict(card.get("comp")).get("scale"),
        card.get("compScale"),
        card.get("scale"),
    )
    merged["company_scale"] = company_scale
    return merged


def main() -> int:
    args = parse_args()
    output_csv = Path(args.output_csv)
    merge_csv = Path(args.merge_csv) if args.merge_csv else None
    db_path = Path(args.db) if args.db else None

    if args.limit <= 0:
        print("[WARN] limit must be positive.")
        write_csv(output_csv, [])
        return 0

    requested_cities = [item.strip() for item in args.cities.split(",") if item.strip()]
    if not requested_cities:
        requested_cities = list(DEFAULT_MAJOR_CITIES)
    elif args.city.strip():
        requested_cities = [args.city.strip()]

    collected_cards = fetch_liepin_search_cards(
        limit=args.limit,
        page_size=args.page_size,
        key=args.key,
        cities=requested_cities,
        dq=args.dq,
        pub_time=args.pub_time,
    )
    if not collected_cards:
        print("[WARN] No recent Liepin jobs found.")
        write_csv(output_csv, [])
        return 0

    if db_path:
        ensure_sqlite_schema(db_path)

    existing_urls = set()
    existing_company_keys: set[str] = set()
    if merge_csv and merge_csv.exists():
        from collectors.collection_utils import load_existing_urls  # local import to avoid circular clutter

        existing_urls = load_existing_urls(merge_csv)
    if db_path and db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            existing_company_keys = load_existing_company_keys(conn, "猎聘")
            existing_urls |= load_existing_urls_from_db(conn)
        finally:
            conn.close()

    captured_at = today_iso()
    rows: list[dict[str, str]] = []
    seen_run_keys: set[str] = set()
    seen_run_urls: set[str] = set()

    for card in collected_cards:
        job_url = extract_job_url(card)
        if not job_url:
            print("[SKIP] Missing job URL in Liepin card")
            continue
        if job_url in seen_run_urls or job_url in existing_urls:
            print(f"[SKIP] Duplicate URL: {job_url}")
            continue

        company_name = card_company_name(card)
        if not company_name:
            print(f"[SKIP] Missing company name: {job_url}")
            continue

        run_key = normalize_dedupe_key(company_name, "猎聘")
        if run_key in seen_run_keys or run_key in existing_company_keys:
            print(f"[SKIP] Duplicate company in this run: {company_name}")
            continue

        try:
            html = fetch_html(job_url)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Fetch failed: {job_url} ({exc})")
            continue

        parsed = enrich_from_card(extract_fields(html, job_url), card)
        if not is_large_company(parsed.get("company_scale", "")):
            print(f"[SKIP] Small/unknown company scale: {company_name} | {parsed.get('company_scale', '')}")
            continue
        if args.skip_no_evidence and not parsed["evidence_quote"]:
            print(f"[SKIP] No evidence quote: {job_url}")
            continue

        row = build_collect_row(
            parsed=parsed,
            captured_at=captured_at,
            collector=args.collector,
            source_platform="猎聘",
            default_risk=args.default_risk,
            index=len(rows) + 1,
            record_prefix="lp",
        )
        rows.append(row)
        seen_run_keys.add(run_key)
        seen_run_urls.add(job_url)
        print(f"[OK] {job_url}")

    write_csv(output_csv, rows)
    print(f"[DONE] Wrote {len(rows)} rows to {output_csv}")

    if merge_csv:
        appended = append_csv(merge_csv, rows)
        print(f"[DONE] Appended {appended} rows to {merge_csv}")

    if db_path:
        inserted = insert_rows_into_db(db_path, rows, "猎聘")
        print(f"[DONE] Inserted {inserted} rows into {db_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
