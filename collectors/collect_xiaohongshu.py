#!/usr/bin/env python3
"""Collect Xiaohongshu posts and comment snippets into CSV/SQLite."""

from __future__ import annotations

import argparse
from csv import DictReader, DictWriter
import html
import json
import os
import re
import sqlite3
import sys
import shutil
import subprocess
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import urllib.request

from collectors.collection_utils import (
    append_csv,
    build_collect_row,
    ensure_sqlite_schema,
    fetch_html,
    read_url_list,
    today_iso,
    write_csv,
)
from collectors.firecrawl_client import FirecrawlError, scrape_url

XHS_SOURCE_PLATFORM = "小红书"
XHS_SOURCE_TYPE = "xiaohongshu"
DEFAULT_COOKIE_PATH = Path(__file__).resolve().parents[1] / "data" / "xiaohongshu-cookie.txt"
DEFAULT_PLAYWRIGHT_CORE_PATH = Path("/tmp/xhs-playwright/node_modules/playwright-core")
DEFAULT_CHROME_PATH = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
PLUGIN_ARTIFACTS_DIR = Path(__file__).resolve().parents[1] / "data" / "xhs-plugin-results"

COMMENT_HEADINGS = (
    "热门评论",
    "最新评论",
    "全部评论",
    "评论",
)

NOISE_LINES = {
    "展开",
    "收起",
    "更多",
    "分享",
    "收藏",
    "点赞",
    "评论",
    "复制链接",
    "小红书",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect Xiaohongshu posts and comments.")
    p.add_argument("--input-urls", required=True, help="Text file with one Xiaohongshu URL per line.")
    p.add_argument("--output-csv", required=True, help="Output CSV path.")
    p.add_argument("--merge-csv", default="", help="Optional CSV to append new rows into.")
    p.add_argument("--db", default="", help="Optional SQLite DB path for direct insertion.")
    p.add_argument("--refresh-url", default="", help="Delete existing rows for this URL before inserting fresh rows.")
    p.add_argument("--collector", default="xiaohongshu", help="Collector identifier.")
    p.add_argument("--source-platform", default=XHS_SOURCE_PLATFORM, help="Source platform label.")
    p.add_argument("--company", default="", help="Optional company override for every row.")
    p.add_argument("--limit", type=int, default=100, help="Maximum URLs to process.")
    p.add_argument("--comment-limit", type=int, default=3, help="Maximum comments to keep per URL; 0 means all available comments.")
    p.add_argument("--api-key", default="", help="Optional Firecrawl API key override.")
    p.add_argument("--base-url", default="https://api.firecrawl.dev", help="Firecrawl API base URL.")
    p.add_argument("--cookie", default="", help="Optional Xiaohongshu cookie header value.")
    p.add_argument("--cookie-file", default="", help="Optional file containing a Cookie header value.")
    p.add_argument(
        "--comment-api-host",
        default="https://edith.xiaohongshu.com",
        help="Xiaohongshu comment API host.",
    )
    p.add_argument("--skip-no-evidence", action="store_true", help="Skip rows without extracted evidence text.")
    p.add_argument(
        "--include-comments",
        action="store_true",
        default=True,
        help="Collect comment snippets in addition to the main post.",
    )
    return p.parse_args()


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
                print(f"[SKIP] Duplicate URL: {row['source_url']}")
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


def delete_rows_for_url(db_path: Path, source_url: str) -> int:
    if not source_url:
        return 0

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(
            """
            DELETE FROM collected_evidence
            WHERE source_url = ?
               OR source_url LIKE ? || '#comment-%'
            """,
            (source_url, source_url),
        )
        conn.commit()
        return cursor.rowcount if cursor.rowcount is not None else 0
    finally:
        conn.close()


def replace_rows_for_url_in_csv(path: Path, source_url: str, new_rows: list[dict[str, str]]) -> int:
    if not path.exists():
        return 0

    retained: list[dict[str, str]] = []
    removed = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = DictReader(f)
        for row in reader:
            row_url = (row.get("source_url") or "").strip()
            if row_url == source_url or row_url.startswith(f"{source_url}#comment-"):
                removed += 1
                continue
            retained.append(row)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = DictWriter(f, fieldnames=retained[0].keys() if retained else new_rows[0].keys() if new_rows else [])
        if writer.fieldnames:
            writer.writeheader()
            writer.writerows(retained)
            writer.writerows(new_rows)
    return removed


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = DictReader(f)
        return [dict(row) for row in reader]


def _build_plugin_artifacts(output_csv: Path) -> dict[str, str | int | list[dict[str, str]]]:
    rows = _load_csv_rows(output_csv)
    artifact_id = f"xhs-{today_iso().replace('-', '')}-{uuid.uuid4().hex[:8]}"
    artifact_dir = PLUGIN_ARTIFACTS_DIR / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    csv_copy = artifact_dir / "result.csv"
    try:
        shutil.copyfile(output_csv, csv_copy)
    except OSError:
        csv_copy = output_csv

    json_path = artifact_dir / "result.json"
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    comment_count = sum(1 for row in rows if "#comment-" in (row.get("source_url") or ""))
    post_count = max(0, len(rows) - comment_count)

    return {
        "artifact_id": artifact_id,
        "download_csv_path": str(csv_copy),
        "download_json_path": str(json_path),
        "record_count": len(rows),
        "post_count": post_count,
        "comment_count": comment_count,
        "preview_rows": rows[:20],
    }


def _strip_tags(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<noscript\b[^>]*>.*?</noscript>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</(p|div|li|section|article|h[1-6])>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_markdown(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = re.sub(r"[`*_>#]+", " ", text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _best_text(*parts: str) -> str:
    for part in parts:
        if part and part.strip():
            return part.strip()
    return ""


def _extract_meta_content(html_text: str, key: str) -> str:
    patterns = [
        rf'<meta[^>]+property=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+name=["\']{re.escape(key)}["\'][^>]+content=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.I | re.S)
        if match:
            return html.unescape(match.group(1)).strip()
    return ""


def _extract_title(raw_html: str, markdown: str, fallback: str) -> str:
    title = _extract_meta_content(raw_html, "og:title")
    if not title:
        match = re.search(r"<title>(.*?)</title>", raw_html, flags=re.I | re.S)
        if match:
            title = _strip_tags(match.group(1))

    if not title and markdown:
        first_line = next((line.strip() for line in markdown.splitlines() if line.strip()), "")
        title = first_line

    title = title or fallback
    title = re.sub(r"\s*[-|｜]\s*(小红书|Xiaohongshu|XHS)$", "", title).strip()
    return title


def _extract_published_at(text: str) -> str:
    patterns = [
        r"20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}日?",
        r"20\d{2}\.\d{1,2}\.\d{1,2}",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = match.group(0)
            value = value.replace("年", "-").replace("月", "-").replace("日", "")
            value = value.replace("/", "-").replace(".", "-")
            return value
    return ""


def _is_noise_line(line: str) -> bool:
    normalized = line.strip()
    if not normalized:
        return True
    if normalized in NOISE_LINES:
        return True
    if re.fullmatch(r"[0-9]+", normalized):
        return True
    if len(normalized) <= 1:
        return True
    if len(normalized) < 6 and not re.search(r"[。！？!?：:，,]", normalized):
        return True
    return False


def _line_candidates(text: str) -> list[str]:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    candidates: list[str] = []
    for line in lines:
        if _is_noise_line(line):
            continue
        normalized = line
        colon_match = re.match(r"^([^：:]{1,24})[：:]\s*(.+)$", normalized)
        if colon_match:
            head = colon_match.group(1).strip()
            tail = colon_match.group(2).strip()
            if head and len(tail) >= 6:
                normalized = tail
        normalized = normalized.strip(" ·•-")
        if _is_noise_line(normalized):
            continue
        candidates.append(normalized)
    return candidates


def _split_post_and_comments(text: str) -> tuple[str, str]:
    if not text:
        return "", ""
    pattern = re.compile(
        r"(?:^|\n)\s*(?:%s)\s*(?:\d+)?\s*(?:\n|$)" % "|".join(re.escape(item) for item in COMMENT_HEADINGS)
    )
    match = pattern.search(text)
    if not match:
        return text, ""
    return text[: match.start()], text[match.end() :]


def _truncate(text: str, limit: int = 140) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _infer_company_name(company_override: str, title: str, body_text: str) -> str:
    if company_override.strip():
        return company_override.strip()

    text = " ".join(part for part in [title, body_text] if part)
    patterns = [
        r"@([^@\s#]{2,40}(?:公司|集团|银行|证券|保险|科技|传媒|教育|文创|电商|品牌|食品|医药|餐饮|酒店|美业|互联网|贸易|制造|物流|地产|物业|商贸))",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            candidate = match.group(1).strip(" -_：:，,。")
            if len(candidate) >= 4 and not re.search(r"(工作|找不到|年龄|面试|招聘|帖子|评论)", candidate):
                return candidate

    return "待补充"


def _scrape_payload(url: str, *, api_key: str, base_url: str) -> dict[str, str]:
    if api_key or os.getenv("FIRECRAWL_API_KEY"):
        try:
            payload = scrape_url(
                url,
                api_key=api_key or None,
                base_url=base_url,
                formats=["markdown", "html"],
                only_main_content=False,
                timeout=120,
            )
            data = payload.get("data") if isinstance(payload, dict) else {}
            data = data if isinstance(data, dict) else {}
            metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
            return {
                "title": str(metadata.get("title") or data.get("title") or ""),
                "markdown": str(data.get("markdown") or ""),
                "html": str(data.get("html") or ""),
                "source": "firecrawl",
            }
        except FirecrawlError as exc:
            print(f"[WARN] Firecrawl scrape failed: {url} ({exc})")

    try:
        raw_html = fetch_html(url, timeout=60)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to fetch URL: {url} ({exc})") from exc

    return {
        "title": "",
        "markdown": "",
        "html": raw_html,
        "source": "html",
    }


def _read_cookie_value(explicit_cookie: str, cookie_file: str) -> str:
    if explicit_cookie.strip():
        return explicit_cookie.strip()
    if cookie_file.strip():
        try:
            return Path(cookie_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            print(f"[WARN] Failed to read cookie file: {exc}")
    if DEFAULT_COOKIE_PATH.exists():
        try:
            return DEFAULT_COOKIE_PATH.read_text(encoding="utf-8").strip()
        except OSError as exc:
            print(f"[WARN] Failed to read default cookie file: {exc}")
    return ""


def _extract_note_id(url: str) -> str:
    parsed = urlparse(url)
    match = re.search(r"/discovery/item/([0-9a-fA-F]+)", parsed.path)
    if match:
        return match.group(1)
    qs = parse_qs(parsed.query)
    return (qs.get("note_id") or qs.get("noteId") or [""])[0].strip()


def _extract_xsec_token(url: str, raw_html: str) -> str:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    token = (qs.get("xsec_token") or qs.get("xsecToken") or [""])[0].strip()
    if token:
        return token
    match = re.search(r'"xsecToken"\s*:\s*"([^"]+)"', raw_html)
    if match:
        return match.group(1).strip()
    return ""


def _xhs_json_get(url: str, *, headers: dict[str, str], timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
        raw = resp.read().decode("utf-8", errors="ignore")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        preview = raw[:300].replace("\n", " ").strip()
        raise RuntimeError(f"XHS comment API returned non-JSON: {preview}") from exc
    return data if isinstance(data, dict) else {}


def _extract_comment_rows_from_data(data: dict, limit: int) -> tuple[list[dict[str, str]], str]:
    if data.get("success") is False or data.get("code") not in (None, 0, "0"):
        msg = str(data.get("msg") or data.get("message") or data.get("code") or "comment API failed")
        return [], msg

    payload = data.get("data") if isinstance(data.get("data"), dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    comments = payload.get("comments") if isinstance(payload.get("comments"), list) else []

    rows: list[dict[str, str]] = []
    for item in comments[:limit]:
        if not isinstance(item, dict):
            continue
        content = _normalize_comment_text(item)
        author = _normalize_comment_author(item.get("user") or item.get("author") or {})
        if not content:
            continue
        rows.append(
            {
                "comment_id": str(item.get("id") or item.get("commentId") or ""),
                "author": author,
                "content": content,
                "created_at": str(item.get("time") or item.get("createTime") or item.get("create_time") or ""),
                "like_count": str(item.get("likedCount") or item.get("likeCount") or "0"),
                "sub_comment_count": str(item.get("subCommentCount") or item.get("replyCount") or "0"),
            }
        )

    return rows, ""


def _build_comment_row(
    *,
    item: dict,
    limit_index: int,
) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None
    content = _normalize_comment_text(item)
    author = _normalize_comment_author(item.get("user") or item.get("author") or item.get("user_info") or {})
    if not content:
        return None
    return {
        "comment_id": str(item.get("id") or item.get("commentId") or ""),
        "author": author,
        "content": content,
        "created_at": str(item.get("time") or item.get("createTime") or item.get("create_time") or ""),
        "like_count": str(item.get("likedCount") or item.get("likeCount") or item.get("like_count") or "0"),
        "sub_comment_count": str(item.get("subCommentCount") or item.get("replyCount") or item.get("sub_comment_count") or "0"),
    }


def _extract_comment_page_info(data: dict) -> tuple[list[dict[str, str]], str, bool]:
    if data.get("success") is False or data.get("code") not in (None, 0, "0"):
        msg = str(data.get("msg") or data.get("message") or data.get("code") or "comment API failed")
        return [], msg, False

    payload = data.get("data") if isinstance(data.get("data"), dict) else {}
    payload = payload if isinstance(payload, dict) else {}
    comments = payload.get("comments") if isinstance(payload.get("comments"), list) else []
    has_more = bool(payload.get("has_more"))

    rows: list[dict[str, str]] = []
    for item in comments:
        row = _build_comment_row(item=item, limit_index=len(rows) + 1)
        if row:
            rows.append(row)
    cursor = str(payload.get("cursor") or "")
    return rows, cursor, has_more


def _find_playwright_core_path() -> str:
    env_path = os.getenv("XHS_PLAYWRIGHT_CORE_PATH", "").strip()
    if env_path:
        candidate = Path(env_path)
        if candidate.exists():
            return str(candidate)

    for candidate in (
        DEFAULT_PLAYWRIGHT_CORE_PATH,
        Path(__file__).resolve().parents[1] / "node_modules" / "playwright-core",
    ):
        if candidate.exists():
            return str(candidate)
    return ""


def _fetch_xhs_comments_via_playwright(
    *, page_url: str, cookie: str, limit: int, timeout: int = 90
) -> tuple[list[dict[str, str]], str]:
    node_bin = shutil.which("node")
    playwright_core_path = _find_playwright_core_path()
    if not node_bin or not playwright_core_path:
        return [], "playwright not available"

    cookie_header = cookie.strip()
    if not cookie_header:
        return [], "missing cookie"

    js = r"""
const https = require('https');
const { chromium } = require(process.env.XHS_PLAYWRIGHT_CORE_PATH);

const pageUrl = process.env.XHS_PAGE_URL;
const chromePath = process.env.XHS_CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const timeoutMs = Number(process.env.XHS_TIMEOUT_MS || '90000');
const cookies = JSON.parse(process.env.XHS_COOKIES_JSON || '[]');
const maxComments = Number(process.env.XHS_MAX_COMMENTS || '0');

function cookieDomain(c) {
  return c.domain || '.xiaohongshu.com';
}

function requestText(requestUrl, headers) {
  return new Promise((resolve, reject) => {
    const req = https.request(requestUrl, { headers }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        resolve({ status: res.statusCode || 0, text: raw });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

(async () => {
  const browser = await chromium.launch({
    executablePath: chromePath,
    headless: true,
  });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1800 },
  });
  await context.addCookies(cookies.map((c) => ({
    name: c.name,
    value: c.value,
    domain: cookieDomain(c),
    path: c.path || '/',
  })));

  const page = await context.newPage();
  const responsePromise = page.waitForResponse((resp) => {
    const u = resp.url();
    return u.includes('/api/sns/web/v2/comment/page') && resp.request().method() === 'GET';
  }, { timeout: timeoutMs });

  await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
  const response = await responsePromise;
  const firstBody = await response.text();
  let data;
  try {
    data = JSON.parse(firstBody);
  } catch (err) {
    throw new Error(`comment api returned non-json: ${firstBody.slice(0, 200)}`);
  }

  const baseRequestHeaders = response.request().headers();
  baseRequestHeaders.cookie = cookies.map((c) => `${c.name}=${c.value}`).join('; ');

  const results = [];
  let cursor = '';
  let hasMore = true;

  while (hasMore) {
    const payload = data && data.data && Array.isArray(data.data.comments) ? data.data : null;
    if (!payload) {
      break;
    }
    results.push(...payload.comments);
    cursor = String(payload.cursor || '');
    hasMore = Boolean(payload.has_more) && Boolean(cursor);
    if (maxComments > 0 && results.length >= maxComments) {
      break;
    }
    if (!hasMore) {
      break;
    }

    const nextUrl = new URL(response.url());
    nextUrl.searchParams.set('cursor', cursor);
    const next = await requestText(nextUrl.toString(), baseRequestHeaders);
    if (next.status !== 200) {
      throw new Error(`comment request failed: ${next.status}`);
    }
    try {
      data = JSON.parse(next.text);
    } catch (err) {
      throw new Error(`comment api returned non-json: ${next.text.slice(0, 200)}`);
    }
  }

  const body = JSON.stringify({
    code: 0,
    success: true,
    msg: 'success',
    data: {
      comments: results,
    },
  });
  process.stdout.write(body);
  await browser.close();
})().catch((err) => {
  process.stderr.write(String(err && err.stack ? err.stack : err));
  process.exit(1);
});
"""
    env = os.environ.copy()
    env["XHS_PLAYWRIGHT_CORE_PATH"] = playwright_core_path
    env["XHS_PAGE_URL"] = page_url
    env["XHS_COOKIES_JSON"] = json.dumps(
        [
            {
                "name": part.split("=", 1)[0].strip(),
                "value": part.split("=", 1)[1],
                "domain": ".xiaohongshu.com",
                "path": "/",
            }
            for part in cookie_header.split(";")
            if "=" in part
        ],
        ensure_ascii=False,
    )
    env["XHS_CHROME_PATH"] = str(DEFAULT_CHROME_PATH)
    env["XHS_TIMEOUT_MS"] = str(timeout * 1000)
    env["XHS_MAX_COMMENTS"] = str(limit if limit > 0 else 0)

    proc = subprocess.run(
        [node_bin, "-e", js],
        capture_output=True,
        text=True,
        timeout=timeout + 30,
        env=env,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        return [], stderr or "playwright comment fetch failed"

    raw = (proc.stdout or "").strip()
    if not raw:
        return [], "empty playwright response"
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return [], f"playwright returned non-JSON: {raw[:200]}"
    rows, _, _ = _extract_comment_page_info(data)
    if limit > 0:
        rows = rows[:limit]
    return rows, ""


def _normalize_comment_text(value: object) -> str:
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    if isinstance(value, dict):
        for key in ("content", "text", "desc", "body"):
            text = _normalize_comment_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        for item in value:
            text = _normalize_comment_text(item)
            if text:
                return text
        return ""
    return ""


def _normalize_comment_author(value: object) -> str:
    if isinstance(value, dict):
        for key in ("nickname", "name", "userName", "user_name", "userNickName"):
            text = _normalize_comment_text(value.get(key))
            if text:
                return text
    return ""


def fetch_xhs_comments(
    *,
    note_id: str,
    xsec_token: str,
    cookie: str,
    api_host: str,
    limit: int,
    timeout: int = 30,
) -> tuple[list[dict[str, str]], str]:
    if not note_id:
        return [], "missing note id"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Referer": f"https://www.xiaohongshu.com/discovery/item/{note_id}",
        "X-Requested-With": "XMLHttpRequest",
    }
    if cookie.strip():
        headers["Cookie"] = cookie.strip()
    rows: list[dict[str, str]] = []
    cursor = ""
    seen_comment_ids: set[str] = set()
    has_more = True
    page_size = max(1, min(20, limit if limit > 0 else 20))

    while has_more:
        params = {
            "noteId": note_id,
            "num": str(page_size),
            "cursor": cursor,
            "imageFormats": "jpg",
            "topCommentId": "",
            "xsecToken": xsec_token,
        }
        url = api_host.rstrip("/") + "/api/sns/web/v2/comment/page?" + urllib.parse.urlencode(params)
        data = _xhs_json_get(url, headers=headers, timeout=timeout)
        page_rows, next_cursor, page_has_more = _extract_comment_page_info(data)
        if data.get("success") is False or data.get("code") not in (None, 0, "0"):
            msg = str(data.get("msg") or data.get("message") or data.get("code") or "comment API failed")
            if "-101" in msg or "登录信息" in msg or "login" in msg.lower():
                return [], msg
            raise RuntimeError(msg)

        for row in page_rows:
            comment_id = row.get("comment_id", "")
            if comment_id and comment_id in seen_comment_ids:
                continue
            if comment_id:
                seen_comment_ids.add(comment_id)
            rows.append(row)
            if limit > 0 and len(rows) >= limit:
                return rows[:limit], ""

        cursor = next_cursor
        has_more = page_has_more and bool(cursor)
        if not has_more:
            break

    return rows, ""


def _build_row(
    *,
    company_name: str,
    source_url: str,
    source_title: str,
    evidence_quote: str,
    evidence_summary: str,
    captured_at: str,
    collector: str,
    source_platform: str,
    published_at: str = "",
    city: str = "",
    job_title: str = "",
    notes: str = "",
    record_index: int,
) -> dict[str, str]:
    parsed = {
        "company_name": company_name,
        "city": city,
        "job_title": job_title,
        "source_title": source_title,
        "evidence_quote": evidence_quote,
        "source_url": source_url,
    }
    row = build_collect_row(
        parsed=parsed,
        captured_at=captured_at,
        collector=collector,
        source_platform=source_platform,
        source_type=XHS_SOURCE_TYPE,
        index=record_index,
    )
    row["record_id"] = f"xhs-{captured_at.replace('-', '')}-{record_index:03d}"
    row["published_at"] = published_at
    row["evidence_summary"] = evidence_summary
    row["notes"] = notes
    return row


def collect_from_url(
    *,
    url: str,
    company_override: str,
    collector: str,
    source_platform: str,
    comment_limit: int,
    include_comments: bool,
    skip_no_evidence: bool,
    api_key: str,
    base_url: str,
    cookie: str,
    comment_api_host: str,
    captured_at: str,
    start_index: int,
) -> tuple[list[dict[str, str]], int]:
    scraped = _scrape_payload(url, api_key=api_key, base_url=base_url)
    raw_html = scraped["html"]
    markdown = scraped["markdown"]
    raw_title = scraped["title"]
    title = _extract_title(raw_html, markdown, raw_title or url)
    meta_description = _extract_meta_content(raw_html, "description")
    text = _best_text(meta_description, markdown, _strip_tags(raw_html))
    text = _normalize_markdown(text) if markdown or meta_description else _strip_tags(raw_html)
    post_text, comments_text = _split_post_and_comments(text)
    note_id = _extract_note_id(url)
    xsec_token = _extract_xsec_token(url, raw_html)

    company_name = _infer_company_name(company_override, title, post_text or text)
    published_at = _extract_published_at(text)

    rows: list[dict[str, str]] = []
    seen_quotes: set[str] = set()
    record_index = start_index

    post_quote = _truncate(post_text or text or title)
    if post_quote and post_quote not in seen_quotes:
        post_row = _build_row(
            company_name=company_name,
            source_url=url,
            source_title=f"{title}（小红书帖子）",
            evidence_quote=post_quote,
            evidence_summary=f"小红书帖子内容：{post_quote}",
            captured_at=captured_at,
            collector=collector,
            source_platform=source_platform,
            published_at=published_at,
            job_title=title,
            notes=f"xhs-post; source={scraped['source']}",
            record_index=record_index,
        )
        rows.append(post_row)
        seen_quotes.add(post_quote)
        record_index += 1

    if include_comments:
        try:
            comment_rows, comment_msg = fetch_xhs_comments(
                note_id=note_id,
                xsec_token=xsec_token,
                cookie=cookie,
                api_host=comment_api_host,
                limit=comment_limit,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Comment API failed: {url} ({exc})")
            comment_rows = []
            comment_msg = str(exc)

        if not comment_rows and cookie.strip():
            browser_rows, browser_msg = _fetch_xhs_comments_via_playwright(
                page_url=url,
                cookie=cookie,
                limit=comment_limit,
            )
            if browser_rows:
                comment_rows = browser_rows
                comment_msg = ""
                print(f"[OK] Browser comment fallback succeeded: {url} ({len(comment_rows)} rows)")
            elif browser_msg:
                print(f"[WARN] Browser comment fallback failed: {url} ({browser_msg})")

        if comment_msg:
            print(f"[WARN] Comment API message: {comment_msg}")
        for idx, comment in enumerate(comment_rows, start=1):
            quote = _truncate(comment.get("content", ""))
            if not quote or quote in seen_quotes:
                continue
            author = comment.get("author", "").strip()
            comment_suffix = f"@{author}" if author else ""
            comment_row = _build_row(
                company_name=company_name,
                source_url=f"{url}#comment-{idx:02d}",
                source_title=f"{title}（小红书评论{comment_suffix}）",
                evidence_quote=quote,
                evidence_summary=f"小红书评论内容：{quote}",
                captured_at=captured_at,
                collector=collector,
                source_platform=source_platform,
                published_at=published_at,
                job_title=title,
                notes=f"xhs-comment; source=api; comment_id={comment.get('comment_id', '')}",
                record_index=record_index,
            )
            rows.append(comment_row)
            seen_quotes.add(quote)
            record_index += 1

    if skip_no_evidence and not rows:
        print(f"[SKIP] No evidence extracted: {url}")
        return [], start_index

    return rows, record_index


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

    cookie = _read_cookie_value(args.cookie, args.cookie_file)

    if db_path:
        ensure_sqlite_schema(db_path)
        if args.refresh_url.strip():
            removed = delete_rows_for_url(db_path, args.refresh_url.strip())
            print(f"[REFRESH] removed {removed} existing rows for {args.refresh_url.strip()}")

    existing_urls: set[str] = set()
    if merge_csv and merge_csv.exists():
        from collectors.collection_utils import load_existing_urls

        existing_urls = load_existing_urls(merge_csv)
    if db_path and db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            existing_urls |= load_existing_urls_from_db(conn)
        finally:
            conn.close()

    refresh_url = args.refresh_url.strip()
    if refresh_url:
        existing_urls.discard(refresh_url)

    captured_at = today_iso()
    rows: list[dict[str, str]] = []
    seen_run_urls: set[str] = set()
    record_index = 1

    for url in urls:
        if url in existing_urls or url in seen_run_urls:
            print(f"[SKIP] Duplicate URL: {url}")
            continue

        try:
            collected_rows, record_index = collect_from_url(
                url=url,
                company_override=args.company,
                collector=args.collector,
                source_platform=args.source_platform,
                comment_limit=max(0, args.comment_limit),
                include_comments=args.include_comments,
                skip_no_evidence=args.skip_no_evidence,
                api_key=args.api_key,
                base_url=args.base_url,
                cookie=cookie,
                comment_api_host=args.comment_api_host,
                captured_at=captured_at,
                start_index=record_index,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Failed to collect {url}: {exc}")
            continue

        if not collected_rows:
            continue

        rows.extend(collected_rows)
        seen_run_urls.add(url)
        print(f"[OK] {url} -> {len(collected_rows)} rows")

    write_csv(output_csv, rows)
    print(f"[DONE] Wrote {len(rows)} rows to {output_csv}")

    if merge_csv:
        if refresh_url:
            removed = replace_rows_for_url_in_csv(merge_csv, refresh_url, rows)
            appended = len(rows)
            print(f"[REFRESH] removed {removed} existing rows from {merge_csv}")
        else:
            appended = append_csv(merge_csv, rows)
        print(f"[DONE] Appended {appended} rows to {merge_csv}")

    if db_path:
        inserted = insert_rows_into_db(db_path, rows)
        print(f"[DONE] Inserted {inserted} rows into {db_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
