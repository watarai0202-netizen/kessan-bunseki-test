from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta, timezone

import streamlit as st

from src.tdnet import fetch_tdnet_items
from src.analyzer import analyze_pdf_to_json, ai_is_enabled
from src.storage import init_db, get_cached_analysis, save_analysis, db_path_default
from src.viz import render_analysis


# ----------------------------
# Constants / Helpers
# ----------------------------
JST = timezone(timedelta(hours=9))

_KESSAN_RE = re.compile(
    r"(決算短信|四半期決算|通期決算|Financial Results|Earnings)",
    re.IGNORECASE
)

def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))

def fmt_dt(dt: datetime | None) -> str:
    if not dt:
        return "不明"
    try:
        return dt.astimezone(JST).strftime("%Y-%m-%d %H:%M JST")
    except Exception:
        return str(dt)

def make_uid(it: dict, i: int) -> str:
    title = (it.get("title") or "").strip()
    code_ = (it.get("code") or "").strip()
    doc_url = (it.get("doc_url") or "").strip()
    link = (it.get("link") or "").strip()
    published = it.get("published_at")
    seed = f"{code_}|{published}|{title}|{doc_url}|{link}|{i}"
    return hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]


# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="決算短信スクリーナー", layout="wide")

# ----------------------------
# Auth (simple password gate)
# ----------------------------
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")
if not APP_PASSWORD:
    st.error("APP_PASSWORD が未設定です（Streamlit Cloud の Secrets に設定してください）")
    st.stop()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("認証が必要です")
    pw = st.text_input("パスワード", type="password")
    if pw == APP_PASSWORD:
        st.session_state.authenticated = True
        st.rerun()
    st.stop()

# ----------------------------
# DB init (cache store)
# ----------------------------
DB_PATH = st.secrets.get("DB_PATH", db_path_default())
init_db(DB_PATH)

MAX_PDF_BYTES = int(st.secrets.get("MAX_PDF_BYTES", 20 * 1024 * 1024))

# ----------------------------
# Header
# ----------------------------
st.title("📈 決算短信スクリーニング & ビジュアライズ")
st.caption("狙い：スマホでも「銘柄→開示→要点＋数値」まで最短で見る。AI要約は押した時だけ実行。")
st.caption(f"PDF上限: {MAX_PDF_BYTES/1024/1024:.1f}MB（超えると解析失敗しやすい）")

# ----------------------------
# Screening controls
# ----------------------------
with st.expander("スクリーニング条件", expanded=True):
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        code = st.text_input("銘柄コード（4桁、空なら直近全体）", value="").strip()
        only_kessan = st.checkbox("決算短信だけに絞る", value=True)

    with col2:
        days = st.slider("直近何日を見る？", 1, 30, 7)
        limit = st.slider("取得件数（大きいほど遅い）", 50, 1000, 300)

    with col3:
        # 最初はOFF推奨（doc_urlが取れてるか確認してからONに）
        only_has_doc_url = st.checkbox("PDF URLがあるものだけ", value=False)
        show_ai_button = st.checkbox("AI分析ボタンを表示", value=True)
        show_debug = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

# sanity for code
if code and (not code.isdigit() or len(code) != 4):
    st.warning("銘柄コードは4桁の数字で入力してください（例：7203）")
    code = ""

# ----------------------------
# Fetch TDnet items
# ----------------------------
cutoff_utc = datetime.now(timezone.utc) - timedelta(days=days)

with st.spinner("開示一覧を取得中..."):
    try:
        items = fetch_tdnet_items(code or None, limit=limit) or []
    except Exception as e:
        st.error(f"TDnet取得エラー: {type(e).__name__}: {e}")
        st.stop()

if show_debug:
    with st.expander("DEBUG: items先頭5件（title/doc_url/linkの確認）", expanded=False):
        st.json(items[:5])

if not items:
    st.info("TDnetから取得できた件数が0です。fetch_tdnet_items の取得先やネットワークを確認してください。")
    st.stop()

# ----------------------------
# Filter
# ----------------------------
filtered: list[dict] = []
for it in items:
    title = (it.get("title") or "").strip()
    doc_url = (it.get("doc_url") or "").strip()
    published = it.get("published_at")

    if only_kessan and (not is_kessan(title)):
        continue
    if only_has_doc_url and not doc_url:
        continue
    if published and published < cutoff_utc:
        continue

    filtered.append(it)

st.subheader(f"候補：{len(filtered)}件")

if not filtered:
    st.info("条件に一致する開示が見つかりませんでした。日数/件数/フィルタを調整してください。")
    st.stop()

# ----------------------------
# AI availability
# ----------------------------
ai_ok = ai_is_enabled()
if show_ai_button and not ai_ok:
    st.warning("Gemini APIキー未設定のため、AI分析は無効です。Secretsに GEMINI_API_KEY を設定してください。")

# ----------------------------
# Render list (mobile-friendly)
# ----------------------------
for i, it in enumerate(filtered[:200]):
    uid = make_uid(it, i)

    title = (it.get("title") or "").strip()
    code_ = (it.get("code") or "").strip()
    doc_url = (it.get("doc_url") or "").strip()
    link = (it.get("link") or "").strip()
    published = it.get("published_at")

    label = f"{code_ or '----'}｜{fmt_dt(published)}｜{title[:60]}"
    with st.expander(label, expanded=False):
        if doc_url:
            st.caption(f"PDF: {doc_url}")
        elif link:
            st.caption(f"Link: {link}（PDF URLが無いのでAI解析不可）")
        else:
            st.caption("URL情報なし（AI解析不可）")

        cached = get_cached_analysis(DB_PATH, doc_url) if doc_url else None
        if cached:
            st.success("解析済み（キャッシュ）")
            render_analysis(cached)
        else:
            st.info("未解析")

        # ボタン作成前に計算しておく（disabledが効く）
        can_run_ai = show_ai_button and ai_ok and bool(doc_url)

        cols = st.columns([1, 1, 3])

        with cols[0]:
            if st.button("キャッシュ表示", key=f"show_{uid}", disabled=(not bool(cached))):
                render_analysis(cached)

        with cols[1]:
            run = st.button("AI分析", key=f"ai_{uid}", disabled=not can_run_ai)

        with cols[2]:
            st.caption("※同じPDF URLはSQLiteに保存し、再解析しません（DBはキャッシュ扱い）。")

        if run:
            with st.spinner("AIが決算短信を解析中..."):
                try:
                    payload = analyze_pdf_to_json(doc_url)
                    save_analysis(DB_PATH, doc_url, code_, title, published, payload)
                    st.success("解析完了")
                    render_analysis(payload)
                except Exception as e:
                    st.error(f"解析エラー: {type(e).__name__}: {e}")

st.divider()

# ----------------------------
# Manual analyze
# ----------------------------
st.subheader("手動解析（URLを貼る）")
st.caption("※まずはPDF URL推奨。HTMLのURLは失敗する場合があります。")

manual = st.text_input("URL（.pdf推奨）", value="").strip()
colA, colB = st.columns([1, 3])
with colA:
    manual_run = st.button("AI解析", disabled=not (ai_ok and manual))
with colB:
    st.caption("Gemini未設定ならSecretsに GEMINI_API_KEY を設定してください。")

if manual_run:
    with st.spinner("AIが解析中..."):
        try:
            payload = analyze_pdf_to_json(manual)
            st.success("解析完了")
            try:
                render_analysis(payload)
            except Exception:
                st.json(payload)
        except Exception as e:
            st.error(f"解析エラー: {type(e).__name__}: {e}")
