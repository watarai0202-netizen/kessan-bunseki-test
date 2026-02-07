from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

TDNET_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"


def _parse_dt_maybe(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    s = str(value).strip().replace("Z", "+00:00")

    # まずISO
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # 次に "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _pick_first(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str):
            vv = v.strip()
            if vv:
                return vv
        else:
            # 数字なども拾う
            try:
                vv = str(v).strip()
                if vv:
                    return vv
            except Exception:
                pass
    return ""


def _unwrap_tdnet(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    APIレスポンスの包みが揺れるので、TDnet相当のdictを確実に取り出す。
    例: {"Tdnet": {...}} / {"TDnet": {...}} / {"tdnet": {...}} / そのまま {...}
    """
    for k in ("TDnet", "Tdnet", "tdnet"):
        v = raw.get(k)
        if isinstance(v, dict):
            return v
    return raw


def _normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    td = _unwrap_tdnet(raw)

    title = _pick_first(td.get("title"), td.get("Title"), td.get("subject"), td.get("Subject"))

    # 銘柄コード：company_code (5桁) が来るケースあり → とりあえず文字列で保持
    code = _pick_first(td.get("code"), td.get("Code"), td.get("company_code"), td.get("ticker"))

    # PDF URL：document_url が確実っぽい（スクショ）
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

    # 詳細リンク（無いこともある）
    link = _pick_first(td.get("url"), td.get("link"), td.get("detail_url"), td.get("detailUrl"))

    published_raw = _pick_first(
        td.get("published_at"),
        td.get("publishedAt"),
        td.get("pubdate"),          # ←スクショでこれ
        td.get("date"),
        td.get("datetime"),
    )
    published_at = _parse_dt_maybe(published_raw)

    return {
        "title": title,
        "code": code,
        "doc_url": doc_url,
        "link": link,
        "published_at": published_at,
        "raw": td,
    }


def fetch_tdnet_items(code: str | None, limit: int = 200) -> List[Dict[str, Any]]:
    if code and code.isdigit() and len(code) in (4, 5):
        url = f"{TDNET_BASE}/{code}.json?limit={limit}"
    else:
        url = f"{TDNET_BASE}/recent.json?limit={limit}"

    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()

    # items以外の形も拾う
    items: Any = None
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
