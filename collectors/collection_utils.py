from __future__ import annotations

import csv
import datetime as dt
import json
import functools
import secrets
import http.cookiejar
import re
import urllib.request
import sqlite3
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote

FIELDNAMES = [
    "record_id",
    "company_name",
    "uscc_or_entity_id",
    "source_type",
    "source_platform",
    "source_url",
    "source_title",
    "published_at",
    "captured_at",
    "city",
    "job_title",
    "evidence_quote",
    "evidence_summary",
    "screenshot_path",
    "collector",
    "verification_status",
    "risk_level",
    "boycott_recommended",
    "notes",
]

AGE_PATTERNS = [
    re.compile(r"[^\n。；]{0,40}(?:\d{2}岁以下|年龄\d{2}岁以下|\d{2}周岁以下)[^\n。；]{0,40}"),
    re.compile(r"[^\n。；]{0,40}(?:年龄要求|年龄限制)[^\n。；]{0,40}"),
]

LIEPIN_JOB_URL_RE = re.compile(r"https?://www\.liepin\.com/job/\d+\.shtml")
LIEPIN_SEARCH_API = "https://api-c.liepin.com/api/com.liepin.searchfront4c.pc-search-job"
LIEPIN_SEARCH_PAGE = "https://www.liepin.com/zhaopin/"
LIEPIN_MOBILE_CITY_PAGE = "https://m.liepin.com/city-{slug}/zhaopin/?scene=seo"
LIEPIN_MOBILE_CITY_HOME = "https://m.liepin.com/city-{slug}/"
DEFAULT_MAJOR_CITIES = ["杭州"]

CITY_SLUG_MAP = {
    "北京": "bj",
    "上海": "sh",
    "深圳": "sz",
    "广州": "gz",
    "杭州": "hz",
    "成都": "cd",
    "南京": "nj",
    "武汉": "wuhan",
    "苏州": "suzhou",
    "天津": "tj",
    "西安": "xian",
    "长沙": "changsha",
    "重庆": "cq",
    "郑州": "zhengzhou",
    "青岛": "qingdao",
    "厦门": "xiamen",
    "宁波": "ningbo",
    "无锡": "wuxi",
    "佛山": "foshan",
    "东莞": "dongguan",
}

LARGE_COMPANY_PATTERNS = [
    re.compile(r"(?:10000人以上|1万以上|5000-10000人|2000-5000人|1000-2000人|500-1000人|1000人以上|500人以上)"),
]


def normalize_company_name(name: str) -> str:
    value = (name or "").strip().lower()
    value = re.sub(r"[\s\u3000·、,，.。()（）\-_/\\]+", "", value)
    return value


def normalize_dedupe_key(company_name: str, source_platform: str = "") -> str:
    # We dedupe at the company level so the same employer only appears once
    # even if it is found across multiple platforms.
    return normalize_company_name(company_name)


def read_url_list(path: Path) -> list[str]:
    urls: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def fetch_html(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
        body = resp.read()
    return body.decode("utf-8", errors="ignore")


def random_token(length: int = 32) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def build_liepin_search_url(
    *,
    page: int,
    page_size: int,
    key: str = "",
    city: str = "",
    dq: str = "",
    pub_time: str = "",
    ck_id: str = "",
    city_slug: str = "",
) -> str:
    base = LIEPIN_SEARCH_PAGE
    if city_slug:
        base = LIEPIN_MOBILE_CITY_PAGE.format(slug=city_slug)
    params = {
        "city": city,
        "dq": dq,
        "pubTime": pub_time,
        "currentPage": str(page),
        "pageSize": str(page_size),
        "key": key,
        "suggestTag": "",
        "workYearCode": "",
        "compId": "",
        "compName": "",
        "compTag": "",
        "industry": "",
        "salary": "",
        "jobKind": "",
        "compScale": "",
        "compKind": "",
        "compStage": "",
        "eduLevel": "",
        "otherCity": "",
        "sfrom": "search_job_pc",
        "ckId": ck_id,
        "scene": "input",
        "skId": ck_id,
        "fkId": ck_id,
        "suggestId": "",
    }
    query = "&".join(f"{k}={quote(v, safe='')}" for k, v in params.items())
    return f"{base}?{query}"


def build_liepin_search_payload(
    *,
    page: int,
    page_size: int,
    key: str = "",
    city: str = "",
    dq: str = "",
    pub_time: str = "",
    ck_id: str = "",
) -> dict:
    return {
        "data": {
            "mainSearchPcConditionForm": {
                "city": city,
                "dq": dq,
                "pubTime": pub_time,
                "currentPage": str(page),
                "pageSize": page_size,
                "key": key,
                "suggestTag": "",
                "workYearCode": "",
                "compId": "",
                "compName": "",
                "compTag": "",
                "industry": "",
                "salaryCode": "",
                "jobKind": "",
                "compScale": "",
                "compKind": "",
                "compStage": "",
                "eduLevel": "",
                "salaryLow": "",
                "salaryHigh": "",
            },
            "passThroughForm": {
                "scene": "seo",
                "skId": ck_id,
                "fkId": ck_id,
                "ckId": ck_id,
            },
        }
    }


@functools.lru_cache(maxsize=None)
def resolve_city_code(city_name: str) -> tuple[str, str]:
    slug = CITY_SLUG_MAP.get(city_name, "")
    if not slug:
        return "", ""

    for candidate in (LIEPIN_MOBILE_CITY_PAGE.format(slug=slug), LIEPIN_MOBILE_CITY_HOME.format(slug=slug)):
        try:
            html = fetch_html(candidate)
        except URLError:
            continue
        match = re.search(r'"dqs"\s*:\s*\{\s*"name"\s*:\s*"([^"]+)"\s*,\s*"code"\s*:\s*"([^"]+)"', html)
        if match:
            return slug, match.group(2)
    return slug, ""


def _make_liepin_opener() -> urllib.request.OpenerDirector:
    cookiejar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookiejar))
    setattr(opener, "_cookiejar", cookiejar)
    return opener


def _find_cookie_value(opener: urllib.request.OpenerDirector, name: str) -> str:
    cookiejar = getattr(opener, "_cookiejar", None)
    if cookiejar is None:
        return ""
    for cookie in cookiejar:
        if cookie.name == name:
            return cookie.value
    return ""


def post_json(
    url: str,
    payload: dict,
    *,
    headers: dict[str, str] | None = None,
    opener: urllib.request.OpenerDirector | None = None,
    timeout: int = 20,
) -> str:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json;charset=UTF-8")
    req.add_header("Accept", "application/json, text/plain, */*")
    if headers:
        for key, value in headers.items():
            if value:
                req.add_header(key, value)
    if opener is None:
        opener = urllib.request.build_opener()
    with opener.open(req, timeout=timeout) as resp:  # nosec B310
        return resp.read().decode("utf-8", errors="ignore")


def _loads_liepin_json(raw: str) -> dict:
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError as exc:
        preview = raw[:300].replace("\n", " ").strip()
        raise RuntimeError(f"Liepin returned non-JSON content: {preview}") from exc
    return loaded if isinstance(loaded, dict) else {}


def extract_job_url(value: object) -> str:
    if isinstance(value, str):
        match = LIEPIN_JOB_URL_RE.search(value)
        return match.group(0) if match else ""
    if isinstance(value, list):
        for item in value:
            found = extract_job_url(item)
            if found:
                return found
        return ""
    if isinstance(value, dict):
        for preferred_key in ("jobUrl", "job_link", "jobLink", "url", "link"):
            found = extract_job_url(value.get(preferred_key))
            if found:
                return found
        for item in value.values():
            found = extract_job_url(item)
            if found:
                return found
    return ""


def extract_liepin_job_cards(payload: dict) -> list[dict]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        inner = data.get("data", {})
        if isinstance(inner, dict):
            cards = inner.get("jobCardList", [])
            if isinstance(cards, list):
                return cards
    cards = data.get("jobCardList", []) if isinstance(data, dict) else []
    return cards if isinstance(cards, list) else []


def is_large_company(scale: str) -> bool:
    normalized = clean_text(scale or "")
    return any(pattern.search(normalized) for pattern in LARGE_COMPANY_PATTERNS)


def fetch_liepin_search_cards(
    *,
    limit: int = 100,
    page_size: int = 40,
    key: str = "",
    cities: list[str] | None = None,
    dq: str = "",
    pub_time: str = "",
    timeout: int = 20,
) -> list[dict]:
    city_list = [city for city in (cities or []) if city]
    if not city_list:
        city_list = [""]

    def _crawl(city: str, start_page: int) -> list[dict]:
        opener = _make_liepin_opener()
        ck_id = random_token(32)
        city_slug, city_code = resolve_city_code(city)
        query_city = city_code or city
        query_dq = city_code or dq
        search_url = build_liepin_search_url(
            page=start_page,
            page_size=page_size,
            key=key,
            city=query_city,
            dq=query_dq,
            pub_time=pub_time,
            ck_id=ck_id,
            city_slug=city_slug,
        )
        try:
            opener.open(search_url, timeout=timeout)  # nosec B310
        except URLError:
            # The search page can be flaky under anti-bot protection; the API
            # request below still works in many cases when the headers are aligned.
            pass

        xsrf = _find_cookie_value(opener, "XSRF-TOKEN")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "X-Client-Type": "web",
            "X-Requested-With": "XMLHttpRequest",
            "X-Fscp-Bi-Stat": json.dumps({"location": search_url}, ensure_ascii=False),
            "X-Fscp-Fe-Version": "",
            "X-Fscp-Std-Info": json.dumps({"client_id": "40108"}, ensure_ascii=False),
            "X-Fscp-Version": "1.1",
            "Referer": search_url,
            "Origin": "https://www.liepin.com",
        }
        if xsrf:
            headers["X-XSRF-TOKEN"] = xsrf

        cards: list[dict] = []
        seen_urls: set[str] = set()
        page = start_page
        while len(cards) < limit:
            page_ck_id = random_token(32)
            page_url = build_liepin_search_url(
                page=page,
                page_size=page_size,
                key=key,
                city=query_city,
                dq=query_dq,
                pub_time=pub_time,
                ck_id=page_ck_id,
                city_slug=city_slug,
            )
            payload = build_liepin_search_payload(
                page=page,
                page_size=page_size,
                key=key,
                city=query_city,
                dq=query_dq,
                pub_time=pub_time,
                ck_id=page_ck_id,
            )
            headers["X-Fscp-Bi-Stat"] = json.dumps({"location": page_url}, ensure_ascii=False)
            headers["Referer"] = page_url

            raw = post_json(LIEPIN_SEARCH_API, payload, headers=headers, opener=opener, timeout=timeout)
            result = _loads_liepin_json(raw)
            if result.get("code") not in (None, "", "0", 0) and result.get("flag") == 0:
                raise RuntimeError(result.get("msg") or f"Liepin search failed: {result.get('code')}")

            page_cards = extract_liepin_job_cards(result)
            if not page_cards:
                break

            for card in page_cards:
                job_url = extract_job_url(card)
                if not job_url or job_url in seen_urls:
                    continue
                cards.append(card)
                seen_urls.add(job_url)
                if len(cards) >= limit:
                    break

            if len(page_cards) < page_size:
                break
            page += 1

        return cards[:limit]

    combined_cards: list[dict] = []
    seen_urls: set[str] = set()
    for city in city_list:
        city_cards = _crawl(city, 0)
        if not city_cards:
            city_cards = _crawl(city, 1)
        for card in city_cards:
            job_url = extract_job_url(card)
            if not job_url or job_url in seen_urls:
                continue
            combined_cards.append(card)
            seen_urls.add(job_url)
            if len(combined_cards) >= limit:
                return combined_cards[:limit]
    return combined_cards[:limit]


def first_match(pattern: str, text: str) -> str:
    m = re.search(pattern, text, flags=re.DOTALL)
    return m.group(1).strip() if m else ""


def clean_text(value: str) -> str:
    value = re.sub(r"\\u003c.*?\\u003e", " ", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def extract_fields(html: str, url: str) -> dict[str, str]:
    title = first_match(r"<title>(.*?)</title>", html)
    job_name = first_match(r'"jobName"\s*:\s*"(.*?)"', html)
    company_name = first_match(r'"companyName"\s*:\s*"(.*?)"', html)
    city = first_match(r'"dq"\s*:\s*"(.*?)"', html)
    company_scale = first_match(r"(?:人数规模|企业规模)[:：]\s*([^\n<]{1,40})", html)
    company_industry = first_match(r"(?:企业行业|所属行业)[:：]\s*([^\n<]{1,60})", html)

    if not company_name and title:
        company_name = title.split("招聘")[0].strip("-_ ")

    soup_text = clean_text(html)
    evidence_quote = ""
    for pattern in AGE_PATTERNS:
        m = pattern.search(soup_text)
        if m:
            evidence_quote = clean_text(m.group(0))
            break

    source_title = (job_name + "招聘（猎聘）").strip()
    if source_title == "招聘（猎聘）":
        source_title = "职位招聘（猎聘）"

    return {
        "company_name": company_name or "",
        "city": city or "",
        "job_title": job_name or "",
        "company_scale": company_scale or "",
        "company_industry": company_industry or "",
        "source_title": source_title,
        "evidence_quote": evidence_quote,
        "source_url": url,
    }


def build_collect_row(
    *,
    parsed: dict[str, str],
    captured_at: str,
    collector: str,
    source_platform: str = "猎聘",
    default_risk: str = "medium",
    index: int = 1,
    source_type: str = "jobsite",
    record_prefix: str = "job",
    run_token: str = "",
) -> dict[str, str]:
    evidence_summary = (
        f"职位页面出现年龄相关表述：{parsed['evidence_quote']}"
        if parsed["evidence_quote"]
        else "未自动抽取到年龄表述，需人工复核页面。"
    )
    token = run_token or captured_at.replace('-', '')
    return {
        "record_id": f"{record_prefix}-{token}-{index:03d}",
        "company_name": parsed["company_name"],
        "uscc_or_entity_id": "",
        "source_type": source_type,
        "source_platform": source_platform,
        "source_url": parsed["source_url"],
        "source_title": parsed["source_title"],
        "published_at": "",
        "captured_at": captured_at,
        "city": parsed["city"],
        "job_title": parsed["job_title"],
        "evidence_quote": parsed["evidence_quote"],
        "evidence_summary": evidence_summary,
        "screenshot_path": "",
        "collector": collector,
        "verification_status": "pending",
        "risk_level": default_risk,
        "boycott_recommended": "false",
        "notes": "auto-collected; pending manual review",
    }


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_existing_urls(path: Path) -> set[str]:
    rows = read_csv_rows(path)
    return {row.get("source_url", "").strip() for row in rows if row.get("source_url")}


def append_csv(path: Path, rows: list[dict[str, str]]) -> int:
    if not rows:
        return 0
    exists = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def ensure_sqlite_schema(db_path: Path) -> None:
    schema_path = Path(__file__).resolve().parents[1] / "db" / "schema.sql"
    if not schema_path.exists():
        raise RuntimeError(f"Schema not found: {schema_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


def today_iso() -> str:
    return dt.date.today().isoformat()
