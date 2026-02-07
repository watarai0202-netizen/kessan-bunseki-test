import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import streamlit as st

from src.tdnet import fetch_tdnet_items
from src.analyzer import analyze_pdf_to_json, ai_is_enabled
from src.storage import init_db, get_cached_analysis, save_analysis, db_path_default
from src.viz import render_analysis

# ----------------------------
# Helpers
# ----------------------------

# 決算っぽいタイトル判定（ゆるめ）
_KESSAN_RE = re.compile(
    r"(決算短信|四半期決算|通期決算|Financial Results|Earnings|Results)",
    re.IGNORECASE,
)

def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))


def _parse_dt_any(value: Any) -> Optional[datetime]:
    """
    published_at の揺れに耐える：
      - ISO: 2026-02-06T20:00:00Z / +09:00
      - スペース区切り: 2026-02-06 20:00:00  (←JST想定)
    返り値はUTC tz-aware datetime
    """
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    # ISO Z 対応
    s_iso = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s_iso)
        if dt.tzinfo is None:
            # tz無しはJST想定（TDnetのpubdateがこのパターン多い）
            dt = dt.replace(tzinfo=timezone(timedelta(hours=9)))
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    # "YYYY-MM-DD HH:MM:SS"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone(timedelta(hours=9)))
            return dt.astimezone(timezone.utc)
        except Exception:
            continue

    return None


def _extract_tdnet_fields(it: Dict[str, Any]) -> Tuple[str, str, str, Optional[datetime]]:
    """
    it の正規化が壊れても app 側で復元する（壊れづらさ優先）。
    戻り: (title, code, doc_url, published_at_utc)
    """
    title = (it.get("title") or "").strip()
    code = str(it.get("code") or "").strip()
    doc_url = (it.get("doc_url") or "").strip()
    published_at = it.get("published_at")

    # published_at が datetime で来てなければパース
    if not isinstance(published_at, datetime):
        published_at = _parse_dt_any(published_at)

    # raw から救済（it["raw"] の下が Tdnet/TDnet/直下 など揺れる）
    raw = it.get("raw") if isinstance(it.get("raw"), dict) else {}
    td = None
    if isinstance(raw.get("Tdnet"), dict):
        td = raw["Tdnet"]
    elif isinstance(raw.get("TDnet"), dict):
        td = raw["TDnet"]
    elif isinstance(raw.get("tdnet"), dict):
        td = raw["tdnet"]
    elif isinstance(raw, dict):
        td = raw

    if isinstance(td, dict):
        if not title:
            title = str(td.get("title") or td.get("Title") or "").strip()

        # 4桁/5桁揺れ：company_code が 45230 みたいに末尾0のことがある
        if not code:
            code = str(td.get("code") or td.get("company_code") or td.get("Code") or "").strip()

        if not doc_url:
            doc_url = str(
                td.get("document_url")
                or td.get("documentUrl")
                or td.get("doc_url")
                or td.get("url")
                or ""
            ).strip()

        if published_at is None:
            published_at = _parse_dt_any(td.get("published_at") or td.get("pubdate") or td.get("date"))

    return title, code, doc_url, published_at


def _code4(code: str) -> str:
    """
    45230 -> 4523 みたいな救済（末尾0が付くパターン用）。
    ただし必ずそうとは限らないので、表示は (元コード) も併記する。
    """
    c = (code or "").strip()
    if len(c) == 5 and c.isdigit() and c.endswith("0"):
        return c[:-1]
    if len(c) >= 4 and c[:4].isdigit():
        return c[:4]
    return c


def _is_allowed_pdf_url(url: str) -> bool:
    """
    手動URL解析の安全策（壊れ防止）。
    - release.tdnet.info のPDF
    - yanoshin rd.php 経由で release.tdnet.info のPDF
    """
    u = (url or "").strip()
    if not u:
        return False
    u_low = u.lower()
    if "release.tdnet.info" in u_low and u_low.endswith(".pdf"):
        return True
    if "webapi.yanoshin.jp/rd.php?" in u_low and "release.tdnet.info" in u_low and ".pdf" in u_low:
        return True
    return False


# ----------------------------
# Page
# ----------------------------
st.set_page_config(page_title="決算短信スクリーナー", layout="wide")

# ----------------------------
# Auth (simple password gate)
# ----------------------------
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")
if not APP_PASSWORD:
    st.error("APP_PASSWORD が未設定です（Streamlit Cloud の Secrets か、ローカルの .streamlit/secrets.toml に設定してください）")
    st.stop()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("認証が必要です")
    pw = st.text_input("パスワード", type="password")
    if pw and pw == APP_PASSWORD:
        st.session_state.authenticated = True
        st.rerun()
    st.stop()

# ----------------------------
# DB init (cache store)
# ----------------------------
DB_PATH = st.secrets.get("DB_PATH", db_path_default())
init_db(DB_PATH)

# ----------------------------
# Header
# ----------------------------
st.title("📈 決算短信スクリーニング & ビジュアライズ")
max_pdf_bytes = int(st.secrets.get("MAX_PDF_BYTES", 0) or 0)
if max_pdf_bytes > 0:
    st.caption(f"狙い：スマホでも『銘柄→決算→要点＋数値』まで最短で見る。AI要約は押した時だけ実行。 / PDF上限: {max_pdf_bytes/1024/1024:.1f}MB")
else:
    st.caption("狙い：スマホでも『銘柄→決算→要点＋数値』まで最短で見る。AI要約は押した時だけ実行。")

# ----------------------------
# Screening controls
# ----------------------------
with st.expander("スクリーニング条件", expanded=True):
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        code_in = st.text_input("銘柄コード（4桁、空なら直近全体）", value="").strip()
        only_kessan = st.checkbox("決算短信だけに絞る（0件なら自動で広めに切替）", value=True)

    with col2:
        days = st.slider("直近何日を見る？", 1, 30, 3)
        limit = st.slider("取得件数（大きいほど遅い）", 50, 500, 200)

    with col3:
        only_has_doc_url = st.checkbox("PDF URLがあるものだけ", value=False)
        show_ai_button = st.checkbox("AI分析ボタンを表示", value=True)
        show_debug = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

# sanity for code
code = ""
if code_in:
    if code_in.isdigit() and len(code_in) == 4:
        code = code_in
    else:
        st.warning("銘柄コードは4桁の数字で入力してください（例：7203）")

# ----------------------------
# Fetch TDnet index (non-scrape)
# ----------------------------
cutoff_utc = datetime.now(timezone.utc) - timedelta(days=days)
with st.spinner("開示一覧を取得中..."):
    items = fetch_tdnet_items(code or None, limit=limit)

if show_debug:
    st.subheader("DEBUG: items 先頭5件（title/code/doc_url/link の揺れ確認）")
    st.json(items[:5])

# ----------------------------
# Normalize + Filter
# ----------------------------
normalized: list[dict[str, Any]] = []
for it in items:
    if not isinstance(it, dict):
        continue
    title, code_raw, doc_url, published_at = _extract_tdnet_fields(it)
    code4 = _code4(code_raw)

    normalized.append(
        {
            "title": title,
            "code": code4,
            "code_raw": code_raw,
            "doc_url": doc_url,
            "published_at": published_at,  # UTC
            "raw": it.get("raw") if isinstance(it.get("raw"), dict) else it,
        }
    )

def apply_filters(use_kessan: bool) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for it in normalized:
        title = it.get("title", "")
        doc_url = (it.get("doc_url") or "").strip()
        published = it.get("published_at")

        if use_kessan and not is_kessan(title):
            continue
        if only_has_doc_url and not doc_url:
            continue
        if isinstance(published, datetime) and published < cutoff_utc:
            continue
        out.append(it)
    return out

filtered = apply_filters(only_kessan)

# 0件なら自動で広めにする
if only_kessan and not filtered:
    st.info("『決算短信だけ』で0件だったので、フィルタを広げて表示します。")
    filtered = apply_filters(False)

st.subheader(f"候補：{len(filtered)}件")
if not filtered:
    st.info("条件に一致する開示が見つかりませんでした。日数や件数、フィルタを調整してください。")
    st.stop()

# AI availability
ai_ok = ai_is_enabled()
if show_ai_button and not ai_ok:
    st.warning("Gemini APIキー未設定のため、AI分析は無効です（数値表示のみ）。Secretsに GEMINI_API_KEY を設定してください。")

# ----------------------------
# Render list
# ----------------------------
# スマホ前提：1件ずつ expander で開く UI
for i, it in enumerate(filtered[:100]):
    title = it.get("title", "")
    code_ = it.get("code", "") or "----"
    code_raw = it.get("code_raw", "") or ""
    doc_url = (it.get("doc_url") or "").strip()
    published = it.get("published_at")

    # 表示用日時（UTC→JST）
    if isinstance(published, datetime):
        published_jst = published.astimezone(timezone(timedelta(hours=9)))
        published_str = published_jst.strftime("%Y-%m-%d %H:%M JST")
    else:
        published_str = "日時不明"

    # 一意キー（DuplicateElementKey対策）
    seed = f"{code_}|{code_raw}|{published_str}|{title}|{doc_url}|{i}"
    uid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

    label = f"{code_}({code_raw})｜{published_str}｜{title}"
    with st.expander(label, expanded=False):
        if doc_url:
            st.caption(f"PDF: {doc_url}")
            st.link_button("PDFを開く", doc_url)
        else:
            st.caption("PDF: （なし）")

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
st.subheader("手動解析（PDF URLを貼る）")
manual = st.text_input("PDF URL（release.tdnet.info の .pdf 推奨）", value="").strip()

colA, colB = st.columns([1, 3])
with colA:
    manual_ok = ai_ok and _is_allowed_pdf_url(manual)
    manual_run = st.button("AI解析", disabled=not manual_ok)

with colB:
    if manual and not _is_allowed_pdf_url(manual):
        st.warning("安全のため、release.tdnet.info のPDF（または yanoshin rd.php 経由）以外はブロックしています。")
    else:
        st.caption("※AI有効＋許可ドメインのPDF URLのみ解析します。")

if manual_run:
    with st.spinner("AIが解析中..."):
        try:
            payload = analyze_pdf_to_json(manual)
            st.success("解析完了")
            render_analysis(payload)
        except Exception as e:
            st.error(f"解析エラー: {type(e).__name__}: {e}")
