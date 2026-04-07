#!/usr/bin/env python3
"""Collect raw job leads from a single Zhaopin search URL."""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import urllib.request
from datetime import datetime
from pathlib import Path

from collectors.collection_utils import (
    append_csv,
    build_collect_row,
    clean_text,
    ensure_sqlite_schema,
    today_iso,
    write_csv,
)

DEFAULT_SEED_URL = "https://www.zhaopin.com/sou/jl489/kw00PG0DASG57EAJGB/p1?cs=6&at=a38fd3bf875b40ffbdc77508ba4ca5fd&rt=a9083c190cd94fa58f994304fc9d21ae&userID=1051054504"
DEFAULT_LIMIT = 100
DEFAULT_COOKIE_PATH = Path(__file__).resolve().parents[1] / "data" / "zhaopin-cookie.txt"
SEARCH_API_URL = "https://fe-api.zhaopin.com/c/i/search/positions"
API_PAGE_SIZE = 20
DEFAULT_SEARCH_PAYLOAD = {
    "S_SOU_FULL_INDEX": "35岁以下",
    "S_SOU_WORK_CITY": "489",
    "order": 0,
    "S_SOU_COMPANY_SCALE": "6",
    "pageSize": API_PAGE_SIZE,
    "pageIndex": 1,
    "eventScenario": "pcSearchedSouSearch",
    "anonymous": 0,
    "sortType": "DEFAULT",
    "platform": 13,
    "version": "0.0.0",
}

ZHILIAN_AGE_PATTERNS = [
    re.compile(r"[^\n。；]{0,30}(?:35岁以下|35周岁以下|年龄要求[^\n。；]{0,20}35|年龄限制[^\n。；]{0,20}35)[^\n。；]{0,30}"),
    re.compile(r"[^\n。；]{0,30}(?:35[^\n。；]{0,10}(?:岁以下|周岁以下))[^\n。；]{0,30}"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect raw job leads from Zhaopin search results.")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--merge-csv", default="", help="Optional CSV to append new rows into.")
    p.add_argument("--db", default="", help="Optional SQLite DB path for direct insertion.")
    p.add_argument("--collector", default="zhilian-default", help="Collector identifier.")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Maximum rows to collect.")
    p.add_argument(
        "--seed-url",
        default=DEFAULT_SEED_URL,
        help="Zhaopin search URL that already contains the requested filters.",
    )
    p.add_argument("--cookie", default="", help="Optional Cookie header value for Zhaopin.")
    p.add_argument("--cookie-file", default="", help="Optional file containing a Cookie header value.")
    return p.parse_args()


def read_cookie(cookie: str, cookie_file: str) -> str:
    if cookie.strip():
        return cookie.strip()
    file_path = Path(cookie_file).expanduser() if cookie_file else DEFAULT_COOKIE_PATH
    try:
        return file_path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def load_existing_urls_from_db(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT source_url FROM collected_evidence").fetchall()
    return {row[0] for row in rows if row[0]}


def insert_rows_into_db(db_path: Path, rows: list[dict[str, str]]) -> int:
    if not rows:
        return 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    inserted = 0
    try:
        existing_urls = load_existing_urls_from_db(conn)
        for row in rows:
            if row["source_url"] in existing_urls:
                continue
            conn.execute(
                """
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
                """,
                row,
            )
            existing_urls.add(row["source_url"])
            inserted += 1
        conn.commit()
    finally:
        conn.close()
    return inserted


def extract_initial_state(html_text: str) -> dict:
    marker = "__INITIAL_STATE__="
    idx = html_text.find(marker)
    if idx < 0:
        raise RuntimeError("智联页面没有找到 __INITIAL_STATE__")
    start = html_text.find("{", idx)
    if start < 0:
        raise RuntimeError("智联页面状态块缺失")

    level = 0
    in_str = False
    escaped = False
    end = None
    for i in range(start, len(html_text)):
        ch = html_text[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                level += 1
            elif ch == "}":
                level -= 1
                if level == 0:
                    end = i + 1
                    break
    if end is None:
        raise RuntimeError("智联页面状态块解析失败")
    return json.loads(html_text[start:end])


def fetch_search_page(*, seed_url: str, page_index: int, page_size: int, cookie: str = "") -> dict:
    payload = dict(DEFAULT_SEARCH_PAYLOAD)
    payload["pageIndex"] = page_index
    payload["pageSize"] = page_size
    req = urllib.request.Request(
        SEARCH_API_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Referer": seed_url,
            **({"Cookie": cookie} if cookie else {}),
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:  # nosec B310
        body = resp.read().decode("utf-8", errors="ignore")
    data = json.loads(body)
    if data.get("code") != 200:
        raise RuntimeError(f"智联搜索接口返回异常: {data.get('code')}")
    return data.get("data") or {}


def extract_position_list(state: dict) -> list[dict]:
    items = state.get("positionList") or []
    return [item for item in items if isinstance(item, dict)]


def parse_position_item(item: dict, *, query_hint: str, index: int, run_token: str) -> dict[str, str]:
    position = ((item.get("jobDetailData") or {}).get("position") or {}).get("base") or {}
    description = (((item.get("jobDetailData") or {}).get("position") or {}).get("desc") or {}).get("description") or ""

    company_name = clean_text(item.get("companyName") or "")
    job_title = clean_text(position.get("positionName") or item.get("name") or "")
    source_url = clean_text(
        item.get("positionUrl")
        or item.get("positionURL")
        or position.get("positionUrl")
        or ""
    )
    company_size = clean_text(item.get("companySize") or "")
    city = clean_text(item.get("workCity") or item.get("cityDistrict") or "")
    summary = clean_text(item.get("jobSummary") or description or "")
    labels = clean_text(" ".join(
        [str(x.get("tag") or "") for x in (item.get("showSkillTags") or []) if isinstance(x, dict)]
        + [str(x.get("value") or "") for x in (item.get("skillLabel") or []) if isinstance(x, dict)]
        + [str(x.get("itemValue") or "") for x in ((item.get("jobKeyword") or {}).get("keywords") or []) if isinstance(x, dict)]
    ))
    evidence_sources = [job_title, summary, description, labels]
    evidence_quote = ""
    for text in evidence_sources:
        if not text:
            continue
        for pattern in ZHILIAN_AGE_PATTERNS:
            match = pattern.search(text)
            if match:
                evidence_quote = clean_text(match.group(0))
                break
        if evidence_quote:
            break

    if not company_name:
        raise ValueError("missing company_name")
    if not job_title:
        job_title = company_name + "招聘"
    if not source_url:
        raise ValueError(f"missing source_url for {company_name}")

    parsed = {
        "company_name": company_name,
        "city": city or "杭州",
        "job_title": job_title,
        "source_title": f"{job_title}（智联招聘）",
        "evidence_quote": evidence_quote,
        "source_url": source_url,
    }

    row = build_collect_row(
        parsed=parsed,
        captured_at=today_iso(),
        collector="zhilian-list",
        source_platform="智联招聘",
        index=index,
        record_prefix="zhl",
        run_token=run_token,
    )
    row["evidence_summary"] = (
        summary[:140]
        if summary
        else "智联招聘搜索列表候选，待后台人工核验。"
    )
    row["notes"] = f"raw-list-candidate; query={query_hint}; company_size={company_size or 'unknown'}; backend-review-needed"
    return row


def collect_zhilian_latest(*, limit: int, seed_url: str, cookie: str = "") -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    page = 1
    total_count = None
    run_token = datetime.now().strftime("%Y%m%d%H%M%S%f")
    max_pages = max(1, (limit + API_PAGE_SIZE - 1) // API_PAGE_SIZE)

    while len(rows) < limit and page <= max_pages:
        print(f"[ZHILIAN] fetching page={page} api={SEARCH_API_URL}", flush=True)
        try:
            data = fetch_search_page(seed_url=seed_url, page_index=page, page_size=API_PAGE_SIZE, cookie=cookie)
            items = data.get("list") or []
            if total_count is None:
                try:
                    total_count = int(data.get("count") or 0)
                except (TypeError, ValueError):
                    total_count = 0
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] 智联 API 抓取失败: {exc}", flush=True)
            break
        if not items:
            print(f"[WARN] 智联第 {page} 页没有列表项。", flush=True)
            break

        query_hint = "35岁以下+10000人以上"
        for item in items:
            try:
                row = parse_position_item(item, query_hint=query_hint, index=len(rows) + 1, run_token=run_token)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] 智联列表项解析失败: {exc}", flush=True)
                continue
            if not row["evidence_quote"]:
                print(f"[SKIP] 智联列表未命中年龄条件: {row['source_url']}", flush=True)
                continue
            if row["source_url"] in seen_urls:
                continue
            seen_urls.add(row["source_url"])
            rows.append(row)
            print(f"[OK] 智联列表: {row['source_url']}", flush=True)
            if len(rows) >= limit:
                break

        pages = 0
        if total_count:
            pages = max(1, (total_count + API_PAGE_SIZE - 1) // API_PAGE_SIZE)
        if pages and page >= pages:
            break
        page += 1

    if not rows:
        print("[WARN] No Zhilian list items found.", flush=True)
    return rows


def main() -> int:
    args = parse_args()
    output_csv = Path(args.output_csv)
    merge_csv = Path(args.merge_csv) if args.merge_csv else None
    db_path = Path(args.db) if args.db else None

    print(f"[ZHILIAN] limit={args.limit} raw-list mode seed={args.seed_url}", flush=True)
    cookie = read_cookie(args.cookie, args.cookie_file)
    rows = collect_zhilian_latest(limit=args.limit, seed_url=args.seed_url, cookie=cookie)

    merge_existing_urls = set()
    db_existing_urls = set()
    if db_path:
        ensure_sqlite_schema(db_path)
    if merge_csv and merge_csv.exists():
        from collectors.collection_utils import load_existing_urls

        merge_existing_urls = load_existing_urls(merge_csv)
    if db_path and db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            db_existing_urls |= load_existing_urls_from_db(conn)
        finally:
            conn.close()

    db_rows: list[dict[str, str]] = []
    merge_rows: list[dict[str, str]] = []
    for row in rows:
        if row["source_url"] not in db_existing_urls:
            db_rows.append(row)
            db_existing_urls.add(row["source_url"])
        if row["source_url"] not in merge_existing_urls:
            merge_rows.append(row)
            merge_existing_urls.add(row["source_url"])

    print(f"[ZHILIAN] db_rows={len(db_rows)} merge_rows={len(merge_rows)}", flush=True)
    write_csv(output_csv, db_rows)
    print(f"[DONE] Wrote {len(db_rows)} rows to {output_csv}", flush=True)

    if merge_csv:
        appended = append_csv(merge_csv, merge_rows)
        print(f"[DONE] Appended {appended} rows to {merge_csv}", flush=True)

    if db_path:
        inserted = insert_rows_into_db(db_path, db_rows)
        print(f"[DONE] Inserted {inserted} rows into {db_path}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
