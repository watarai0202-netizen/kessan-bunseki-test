from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

TDNET_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"


def _parse_dt_maybe(value: str | None) -> datetime | None:
    if not value:
        return None
    s = value.strip().replace("Z", "+00:00")
    # よくある "2025-02-07 12:34:56" 形式も拾う
    for fmt in (None, "%Y-%m-%d %H:%M:%S"):
        try:
            if fmt:
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None


def _pick_first(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    td = raw.get("TDnet") if isinstance(raw.get("TDnet"), dict) else raw

    title = _pick_first(td.get("title"), td.get("Title"), td.get("subject"), td.get("Subject"))
    code = _pick_first(td.get("code"), td.get("Code"), td.get("ticker"), td.get("Ticker"))

    # URLキーが揺れるのを最大限拾う（PDF優先）
    doc_url = _pick_first(
        td.get("document_url"),
        td.get("documentUrl"),
        td.get("doc_url"),
        td.get("pdf_url"),
        td.get("pdfUrl"),
        td.get("pdf"),
        td.get("attachment_url"),
        td.get("attachmentUrl"),
    )

    # 「url」「link」がHTMLの詳細ページを指すことが多いので別で保持
    link = _pick_first(td.get("link"), td.get("Link"), td.get("url"), td.get("detail_url"), td.get("detailUrl"))

    published_raw = _pick_first(
        td.get("published_at"),
        td.get("publishedAt"),
        td.get("pubdate"),
        td.get("date"),
        td.get("datetime"),
    )
    published_at = _parse_dt_maybe(published_raw)

    return {
        "title": title,
        "code": code,
        "doc_url": doc_url,   # PDF直リンクが取れたら入る
        "link": link,         # 取れない場合の保険
        "published_at": published_at,
        "raw": td,
    }


def fetch_tdnet_items(code: str | None, limit: int = 200) -> List[Dict[str, Any]]:
    if code and code.isdigit() and len(code) == 4:
        url = f"{TDNET_BASE}/{code}.json?limit={limit}"
    else:
        url = f"{TDNET_BASE}/recent.json?limit={limit}"

    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()

    # items 以外の形も拾う（壊れにくさ）
    items = None
    if isinstance(data, dict):
        items = data.get("items") or data.get("data") or data.get("result")
    elif isinstance(data, list):
        items = data

    if not isinstance(items, list):
        return []

    out: List[Dict[str, Any]] = []
    for raw in items:
        if isinstance(raw, dict):
            out.append(_normalize_item(raw))
    return out
