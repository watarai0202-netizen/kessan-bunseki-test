from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

import requests

TDNET_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"

# "2026-02-06 20:00:00" みたいな naive が来たら JST 扱いにして UTC へ
JST = timezone(timedelta(hours=9))


def _parse_dt_maybe(value: str | None) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JST)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _pick_tdnet_dict(raw: dict[str, Any]) -> dict[str, Any]:
    """
    APIレスポンスのキー揺れ対策：
    - "TDnet" / "Tdnet" / "tdnet" のどれで来ても拾う
    - どれも無ければ raw 自体を返す
    """
    for k in ("TDnet", "Tdnet", "tdnet"):
        v = raw.get(k)
        if isinstance(v, dict):
            return v
    return raw


def _normalize_item(raw: dict[str, Any]) -> dict[str, Any]:
    td = _pick_tdnet_dict(raw)

    title = td.get("title") or td.get("Title") or ""
    code = td.get("code") or td.get("Code") or ""

    # 会社コード/社名（yanoshin側のキーに合わせる）
    company_code = td.get("company_code") or td.get("companyCode") or code or ""
    company_name = td.get("company_name") or td.get("companyName") or ""

    # 4桁コードを作る（例: 45230 → 5230）
    code4 = ""
    cc = str(company_code or "").strip()
    if cc.isdigit():
        code4 = cc[-4:]
    else:
        c = str(code or "").strip()
        if c.isdigit():
            code4 = c[-4:]

    # URLキーが揺れた場合に備える
    doc_url = (
        td.get("document_url")
        or td.get("documentUrl")
        or td.get("doc_url")
        or td.get("url")
        or ""
    )

    published_raw = td.get("published_at") or td.get("pubdate") or td.get("date") or ""
    published_at = _parse_dt_maybe(published_raw)

    return {
        "title": str(title),
        "code": str(code) if code is not None else "",
        "company_code": str(company_code),
        "code4": str(code4),
        "company_name": str(company_name),
        "doc_url": str(doc_url),
        "published_at": published_at,
        "raw": td,
    }


def fetch_tdnet_items(code: str | None, limit: int = 200) -> list[dict[str, Any]]:
    """
    code があれば銘柄別、なければ recent。
    """
    if code and code.isdigit() and len(code) == 4:
        url = f"{TDNET_BASE}/{code}.json?limit={limit}"
    else:
        url = f"{TDNET_BASE}/recent.json?limit={limit}"

    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()

    items = data.get("items")
    if not isinstance(items, list):
        return []

    out: list[dict[str, Any]] = []
    for raw in items:
        if isinstance(raw, dict):
            out.append(_normalize_item(raw))
    return out
