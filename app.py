import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import streamlit as st

from src.tdnet import fetch_tdnet_items
from src.analyzer import analyze_pdf_to_json, ai_is_enabled
from src.storage import init_db, get_cached_analysis, save_analysis, db_path_default
from src.viz import render_analysis

# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="決算短信スクリーナー", layout="wide")

# ----------------------------
# Auth (simple password gate)
# ----------------------------
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")
if not APP_PASSWORD:
    st.error("APP_PASSWORD が未設定です（Streamlit CloudのSecretsに設定してください）")
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

# ----------------------------
# Regex
# ----------------------------
_RE_KESSAN_STRICT = re.compile(r"(決算短信)", re.IGNORECASE)
_RE_KESSAN_WIDE = re.compile(
    r"(決算短信|四半期|通期|決算説明|Financial Results|Earnings|Results|業績|業績予想|売上収益|月次)",
    re.IGNORECASE,
)

def is_kessan_strict(title: str) -> bool:
    return bool(_RE_KESSAN_STRICT.search(title or ""))

def is_kessan_wide(title: str) -> bool:
    return bool(_RE_KESSAN_WIDE.search(title or ""))

# ----------------------------
# Helpers (壊れにくさ最優先)
# ----------------------------
def _parse_dt_any(v: Any) -> Optional[datetime]:
    if not v:
        return None
    s = str(v).strip().replace("Z", "+00:00")

    # ISO
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # "YYYY-MM-DD HH:MM:SS"
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def _unwrap_raw(it: Dict[str, Any]) -> Dict[str, Any]:
    """
    it["raw"] が {"Tdnet": {...}} / {"TDnet": {...}} / {"tdnet": {...}} のように包まれているケースや
    it自体がそれに近いケースでも、中身dictを取り出す。
    """
    raw = it.get("raw")
    if isinstance(raw, dict):
        for k in ("TDnet", "Tdnet", "tdnet"):
            if isinstance(raw.get(k), dict):
                return raw.get(k)
        return raw

    # 念のため it 自体も見る
    for k in ("TDnet", "Tdnet", "tdnet"):
        if isinstance(it.get(k), dict):
            return it.get(k)

    return it

def _pick_first(*vals: Any) -> str:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str):
            vv = v.strip()
            if vv:
                return vv
        else:
            try:
                vv = str(v).strip()
                if vv:
                    return vv
            except Exception:
                pass
    return ""

def normalize_in_app(it: Dict[str, Any]) -> Dict[str, Any]:
    """
    src/tdnet.py が壊れても表示が死なないように、
    it と raw の両方から必要フィールドを保守的に復元する。
    """
    td = _unwrap_raw(it)

    title = _pick_first(
        it.get("title"),
        td.get("title"),
        td.get("Title"),
        td.get("subject"),
        td.get("Subject"),
    )

    code = _pick_first(
        it.get("code"),
        td.get("code"),
        td.get("Code"),
        td.get("company_code"),   # ←スクショでこれ
        td.get("ticker"),
    )

    doc_url = _pick_first(
        it.get("doc_url"),
        td.get("document_url"),   # ←スクショでこれ
        td.get("documentUrl"),
        td.get("doc_url"),
        td.get("pdf_url"),
        td.get("url"),
    ).strip()

    link = _pick_first(
        it.get("link"),
        td.get("link"),
        td.get("url"),
        td.get("detail_url"),
    ).strip()

    published = it.get("published_at")
    if not isinstance(published, datetime):
        published = _parse_dt_any(
            it.get("published_at")
        ) or _parse_dt_any(
            td.get("published_at")
        ) or _parse_dt_any(
            td.get("pubdate")  # ←スクショでこれ
        ) or _parse_dt_any(
            td.get("date")
        )

    # 表示用の銘柄コード：5桁の場合は末尾4桁を併記（好みで）
    code_disp = code
    if code.isdigit() and len(code) == 5:
        code_disp = f"{code[-4:]}({code})"

    # 安全な uid（button key重複を潰す）
    seed_parts = [
        _pick_first(td.get("id"), it.get("id"), ""),
        code,
        str(published) if published else "",
        title,
        doc_url,
        link,
    ]
    seed = "|".join(seed_parts)
    uid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

    return {
        "title": title,
        "code": code,
        "code_disp": code_disp,
        "doc_url": doc_url,
        "link": link,
        "published_at": published,
        "uid": uid,
        "raw": td,
    }

def is_allowed_final_pdf_host(final_url: str) -> bool:
    host = urlparse(final_url).netloc.lower()
    return host.endswith("release.tdnet.info")

# ----------------------------
# Header
# ----------------------------
st.title("📈 決算短信スクリーニング & ビジュアライズ")
st.caption("狙い：スマホでも「銘柄→開示→要点＋数値」まで最短で見る。AI要約は押した時だけ実行。")
st.caption("※ PDF上限は Secrets の MAX_PDF_BYTES で制御（未設定なら analyzer 側のデフォルト）")

# ----------------------------
# Screening controls
# ----------------------------
with st.expander("スクリーニング条件", expanded=True):
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        code_in = st.text_input("銘柄コード（空なら直近全体）", value="").strip()
        only_kessan = st.checkbox("決算短信だけに絞る（0件なら自動で広めに切替）", value=False)

    with col2:
        days = st.slider("直近何日を見る？", 1, 30, 12)
        limit = st.slider("取得件数（大きいほど遅い）", 50, 800, 300)

    with col3:
        only_has_doc_url = st.checkbox("PDF URLがあるものだけ", value=False)
        show_ai_button = st.checkbox("AI分析ボタンを表示", value=True)
        debug_show = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

# ----------------------------
# Fetch TDnet index
# ----------------------------
cutoff_utc = datetime.now(timezone.utc) - timedelta(days=days)
with st.spinner("開示一覧を取得中..."):
    items_raw = fetch_tdnet_items(code_in or None, limit=limit)

# app側で最終正規化（保険）
items = [normalize_in_app(it) for it in items_raw]

if debug_show:
    st.subheader("DEBUG: 取得状況")
    st.write(
        {
            "items_total": len(items),
            "has_title": sum(1 for x in items if x["title"]),
            "has_doc_url": sum(1 for x in items if x["doc_url"]),
            "has_published": sum(1 for x in items if x["published_at"] is not None),
        }
    )
    st.json(items[:5])

# ----------------------------
# Filter builder
# ----------------------------
def build_filtered(use_strict: bool) -> list[dict[str, Any]]:
    out = []
    for it in items:
        title = it.get("title") or ""
        doc_url = (it.get("doc_url") or "").strip()
        published = it.get("published_at")

        if only_kessan:
            ok = is_kessan_strict(title) if use_strict else is_kessan_wide(title)
            if not ok:
                continue

        if only_has_doc_url and not doc_url:
            continue

        if isinstance(published, datetime) and published < cutoff_utc:
            continue

        out.append(it)
    return out

# strict -> fallback to wide if zero
filtered = build_filtered(use_strict=True)
if only_kessan and len(filtered) == 0:
    st.warning("決算短信（厳密）では0件でした。決算関連（広め）に自動切替して表示します。")
    filtered = build_filtered(use_strict=False)

# ----------------------------
# AI availability
# ----------------------------
ai_ok = ai_is_enabled()
if show_ai_button and not ai_ok:
    st.warning("Gemini APIキー未設定のため、AI分析は無効です（表示のみ）。Secretsに GEMINI_API_KEY を設定してください。")

# ----------------------------
# Render list
# ----------------------------
st.subheader(f"候補：{len(filtered)}件")
if not filtered:
    st.info("条件に一致する開示が見つかりませんでした。日数/件数/フィルタを調整してください。")
    st.stop()

# スマホ前提：1件ずつexpander
for i, it in enumerate(filtered[:200]):  # 表示上限（重くなるので）
    title = it.get("title", "")
    code_disp = it.get("code_disp", "") or "----"
    doc_url = (it.get("doc_url") or "").strip()
    published = it.get("published_at")
    uid = it.get("uid") or hashlib.md5(f"{i}".encode()).hexdigest()[:12]

    published_str = published.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if isinstance(published, datetime) else "不明"
    label = f"{code_disp}｜{published_str}｜{title or '(タイトル不明)'}"

    with st.expander(label, expanded=False):
        if doc_url:
            st.caption(f"PDF: {doc_url}")
        else:
            st.caption("URL情報なし（AI解析不可）")

        cached = get_cached_analysis(DB_PATH, doc_url) if doc_url else None
        if cached:
            st.success("解析済み（キャッシュ）")
            render_analysis(cached)
        else:
            st.info("未解析")

        cols = st.columns([1, 1, 2])

        with cols[0]:
            if st.button("キャッシュ表示", key=f"show_{uid}") and cached:
                render_analysis(cached)

        with cols[1]:
            can_run_ai = show_ai_button and ai_ok and bool(doc_url)
            run = st.button("AI分析", key=f"ai_{uid}", disabled=not can_run_ai)

        with cols[2]:
            st.caption("※同じPDF URLはSQLiteに保存し、再解析しません（DBはキャッシュ扱い）。")

        if run:
            with st.spinner("AIが決算資料を解析中..."):
                try:
                    payload = analyze_pdf_to_json(doc_url)
                    save_analysis(
                        DB_PATH,
                        doc_url,
                        it.get("code", ""),
                        title,
                        published,
                        payload,
                    )
                    st.success("解析完了")
                    render_analysis(payload)
                except Exception as e:
                    st.error(f"解析エラー: {type(e).__name__}: {e}")

st.divider()

# Manual analyze
st.subheader("手動解析（PDF URLを貼る）")
manual = st.text_input("PDF URL（.pdf推奨）", value="").strip()
colA, colB = st.columns([1, 3])
with colA:
    manual_run = st.button("AI解析", disabled=not (ai_ok and manual))
with colB:
    st.caption("※PDF以外のURLだと失敗します（HTMLなど）。")

if manual_run:
    with st.spinner("AIが解析中..."):
        payload = analyze_pdf_to_json(manual)
    st.json(payload)
