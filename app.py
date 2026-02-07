from __future__ import annotations

import base64
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

import requests
import streamlit as st

from src.tdnet import fetch_tdnet_items  # ← ここがポイント（tdnet_apiではなくtdnet）


APP_TITLE = "決算短信スクリーニング＆ビジュアライズ"
DB_PATH = "app.db"

_KESSAN_RE = re.compile(r"(決算短信|四半期決算|通期決算|Financial Results|Earnings)", re.IGNORECASE)


def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_secret(key: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(key, default))
    except Exception:
        return default


APP_PASSWORD = get_secret("APP_PASSWORD", "")
GEMINI_API_KEY = get_secret("GEMINI_API_KEY", "")
MAX_PDF_BYTES = int(get_secret("MAX_PDF_BYTES", "20000000") or "20000000")  # default 20MB


@st.cache_resource
def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cache (
            pdf_url TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            model TEXT,
            result_text TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def db_get(pdf_url: str) -> Optional[dict[str, Any]]:
    conn = get_conn()
    cur = conn.execute("SELECT pdf_url, created_at, model, result_text FROM cache WHERE pdf_url=?", (pdf_url,))
    row = cur.fetchone()
    if not row:
        return None
    return {
        "pdf_url": row[0],
        "created_at": row[1],
        "model": row[2],
        "result_text": row[3],
    }


def db_set(pdf_url: str, result_text: str, model: str = "") -> None:
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO cache (pdf_url, created_at, model, result_text) VALUES (?, ?, ?, ?)",
        (pdf_url, utcnow().isoformat(), model, result_text),
    )
    conn.commit()


def is_allowed_tdnet_pdf_url(url: str) -> bool:
    if not url:
        return False
    try:
        u = urlparse(url)
        host = (u.hostname or "").lower()

        if host.endswith("release.tdnet.info"):
            return True

        if host.endswith("webapi.yanoshin.jp") and u.path.endswith("/rd.php"):
            qs = parse_qs(u.query)
            for k in ("url", "u", "target"):
                if k in qs and qs[k]:
                    t = qs[k][0]
                    th = (urlparse(t).hostname or "").lower()
                    return th.endswith("release.tdnet.info")
            q = u.query
            if q.startswith("http"):
                th = (urlparse(q).hostname or "").lower()
                return th.endswith("release.tdnet.info")

        return False
    except Exception:
        return False


def fetch_pdf_bytes(pdf_url: str) -> bytes:
    if not is_allowed_tdnet_pdf_url(pdf_url):
        raise ValueError("許可されていないPDF URLです（release.tdnet.info のみ許可）")

    r = requests.get(pdf_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}, stream=True)
    r.raise_for_status()

    chunks = []
    total = 0
    for chunk in r.iter_content(chunk_size=1024 * 128):
        if not chunk:
            continue
        total += len(chunk)
        if total > MAX_PDF_BYTES:
            raise ValueError(f"PDFが大きすぎます（上限 {MAX_PDF_BYTES:,} bytes）")
        chunks.append(chunk)
    return b"".join(chunks)


def gemini_analyze_pdf(pdf_bytes: bytes, prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY が未設定です")

    model = "gemini-1.5-flash"
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={GEMINI_API_KEY}"

    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": prompt},
                    {"inline_data": {"mime_type": "application/pdf", "data": pdf_b64}},
                ],
            }
        ],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048},
    }

    resp = requests.post(endpoint, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        return json.dumps(data, ensure_ascii=False, indent=2)


def fmt_dt(dt: Any) -> str:
    if not dt:
        return "----"
    if isinstance(dt, datetime):
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return str(dt)


def build_code_label(it: dict[str, Any]) -> str:
    code4 = (it.get("code4") or "").strip()
    company_code = (it.get("company_code") or it.get("code") or "").strip()
    company_name = (it.get("company_name") or "").strip()

    base = code4 or company_code or "----"
    if company_code and company_code != base:
        base = f"{base}({company_code})"
    if company_name:
        base = f"{base} {company_name}"
    return base


def make_uid(i: int, it: dict[str, Any]) -> str:
    title = it.get("title", "")
    code_ = it.get("code4") or it.get("company_code") or it.get("code") or ""
    doc_url = (it.get("doc_url") or "").strip()
    published = it.get("published_at")
    seed = f"{code_}|{published}|{title}|{doc_url}|{i}"
    return hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]


def ensure_login() -> bool:
    if not APP_PASSWORD:
        st.warning("APP_PASSWORD が未設定です（Secretsに設定してください）")
        return False

    if st.session_state.get("authed") is True:
        return True

    pw = st.text_input("パスワード", type="password", placeholder="APP_PASSWORD を入力")
    if pw and pw == APP_PASSWORD:
        st.session_state["authed"] = True
        st.success("ログインOK")
        return True

    st.info("ログインしてください")
    return False


st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
st.caption("狙い：スマホでも「銘柄→開示→要点＋数値」まで最短で見る。AI要約は押した時だけ実行。")
st.caption(f"PDF上限: {MAX_PDF_BYTES/1_000_000:.1f}MB（超えると解析失敗しやすい）")

if not ensure_login():
    st.stop()

with st.expander("スクリーニング条件", expanded=True):
    colL, colM, colR = st.columns([2, 2, 2])

    with colL:
        code_input = st.text_input("銘柄コード（4桁、空なら直近全体）", value="", placeholder="例：8170")
        only_kessan = st.checkbox("決算短信だけに絞る（0件なら自動で広めに切替）", value=True)

    with colM:
        days = st.slider("直近何日を見る？", min_value=1, max_value=30, value=12, step=1)
        limit = st.slider("取得件数（大きいほど遅い）", min_value=50, max_value=500, value=300, step=10)

    with colR:
        only_pdf = st.checkbox("PDF URLがあるものだけ", value=False)
        show_ai_btn = st.checkbox("AI分析ボタンを表示", value=True)
        debug_show = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

can_run_ai = bool(GEMINI_API_KEY) and show_ai_btn
if show_ai_btn and not GEMINI_API_KEY:
    st.warning("GEMINI_API_KEY が未設定なのでAI分析は無効です（StreamlitのSecretsに入れてください）")

try:
    items = fetch_tdnet_items(code_input.strip() if code_input else None, limit=int(limit))
except Exception as e:
    st.error(f"TDnet取得に失敗: {e}")
    st.stop()

if debug_show:
    st.write("DEBUG: items先頭5件（title/doc_url/published/company_nameの確認）")
    st.json(items[:5])

cutoff = utcnow() - timedelta(days=int(days))
filtered = []
for it in items:
    published = it.get("published_at")
    if isinstance(published, datetime):
        if published < cutoff:
            continue
    filtered.append(it)

if only_pdf:
    filtered = [it for it in filtered if (it.get("doc_url") or "").strip()]

if only_kessan:
    k = [it for it in filtered if is_kessan(it.get("title", ""))]
    if len(k) != 0:
        filtered = k
    # 0件なら自動で広げる（何もしない）

st.subheader(f"候補：{len(filtered)}件")

if len(filtered) == 0:
    st.info("条件に一致する開示が見つかりませんでした。日数/件数/フィルタを調整してください。")
    st.stop()

max_show = min(100, len(filtered))
for i, it in enumerate(filtered[:max_show]):
    uid = make_uid(i, it)

    title = it.get("title", "") or ""
    doc_url = (it.get("doc_url") or "").strip()
    published = it.get("published_at")

    code_label = build_code_label(it)
    header_left = f"{code_label} | {fmt_dt(published)}"
    header = f"{header_left} | {title}"

    with st.expander(header, expanded=False):
        if doc_url:
            st.markdown(f"PDF: {doc_url}")
        else:
            st.caption("URL情報なし（AI解析不可）")

        cached = db_get(doc_url) if doc_url else None
        if cached:
            st.success("解析済み（キャッシュあり）")
        else:
            st.info("未解析")

        cols = st.columns([1, 1, 3])

        with cols[0]:
            if st.button("キャッシュ表示", key=f"show_{uid}", disabled=not bool(doc_url)):
                c = db_get(doc_url) if doc_url else None
                if not c:
                    st.warning("キャッシュがありません")
                else:
                    st.caption(f"cached_at: {c['created_at']}  model: {c.get('model','')}")
                    st.text_area("cache", c["result_text"], height=240)

        with cols[1]:
            run = st.button(
                "AI分析",
                key=f"ai_{uid}",
                disabled=(not can_run_ai) or (not bool(doc_url)),
            )

        with cols[2]:
            if cached:
                st.text_area("結果", cached["result_text"], height=240, key=f"res_{uid}", disabled=True)

        if run:
            if not doc_url:
                st.error("PDF URLが無いので解析できません")
            elif not is_allowed_tdnet_pdf_url(doc_url):
                st.error("許可されていないPDF URLです（release.tdnet.info のみ許可）")
            else:
                c = db_get(doc_url)
                if c:
                    st.success("キャッシュがあるので再解析しません")
                    st.text_area("結果", c["result_text"], height=280, key=f"res2_{uid}", disabled=True)
                else:
                    try:
                        with st.spinner("PDF取得中..."):
                            pdf_bytes = fetch_pdf_bytes(doc_url)

                        prompt = """あなたは日本株の決算短信を読むアナリストです。
このPDF（TDnetの開示資料）を読み、以下を日本語で簡潔にまとめてください。

1) 結論：好材料/悪材料/中立（理由を一言）
2) 売上・営業利益・経常利益・純利益（前年同期比/前年差が分かれば）
3) 会社のコメント要旨（増減要因）
4) 通期（または次期）見通しの変更有無
5) 株価材料になりうる論点（最大5つ）
6) 注意点（特殊要因、為替、減損、特損など）

※数値は見つけた範囲でOK。無理に推測しない。
"""
                        with st.spinner("AI分析中..."):
                            text = gemini_analyze_pdf(pdf_bytes, prompt)
                        db_set(doc_url, text, model="gemini-1.5-flash")
                        st.success("解析してキャッシュしました")
                        st.text_area("結果", text, height=320, key=f"res3_{uid}", disabled=True)
                    except Exception as e:
                        st.error(f"AI分析に失敗: {e}")

st.caption("※同じPDF URLはSQLiteに保存し、再解析しません（DBはキャッシュ扱い）。")
