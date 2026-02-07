from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import streamlit as st

from src.tdnet import fetch_tdnet_items
from src.analyzer import summarize_kessan_pdf

APP_TITLE = "決算短信スクリーニング＆ビジュアライズ"
DB_PATH = "app.db"

_KESSAN_RE = re.compile(r"(決算短信|四半期決算|通期決算|Financial Results|Earnings)", re.IGNORECASE)


def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))


def get_secret(name: str, default: str = "") -> str:
    try:
        v = st.secrets.get(name)
        if v is None:
            return default
        return str(v).strip()
    except Exception:
        return default


def init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analysis_cache (
            key TEXT PRIMARY KEY,
            pdf_url TEXT,
            code TEXT,
            company_name TEXT,
            title TEXT,
            published_at TEXT,
            result TEXT,
            created_at TEXT
        )
        """
    )
    con.commit()
    con.close()


def cache_key(pdf_url: str) -> str:
    return hashlib.sha256((pdf_url or "").encode("utf-8")).hexdigest()


def get_cached(pdf_url: str) -> Optional[dict[str, Any]]:
    k = cache_key(pdf_url)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM analysis_cache WHERE key = ?", (k,))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    return dict(row)


def set_cached(pdf_url: str, code: str, company_name: str, title: str, published_at: str, result: str) -> None:
    k = cache_key(pdf_url)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO analysis_cache
        (key, pdf_url, code, company_name, title, published_at, result, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (k, pdf_url, code, company_name, title, published_at, result, datetime.now(timezone.utc).isoformat()),
    )
    con.commit()
    con.close()


def unwrap_tdnet_pdf(url: str) -> str:
    """
    src/tdnet.py側でもunwrapしてるが、UI側でも念のため。
    """
    u = (url or "").strip()
    if not u:
        return ""
    if "webapi.yanoshin.jp/rd.php?" in u:
        try:
            return u.split("rd.php?", 1)[1].strip()
        except Exception:
            return u
    return u


def is_pdf_url(url: str) -> bool:
    u = (url or "").strip().lower()
    return u.endswith(".pdf") or ".pdf?" in u


def within_days(dt: Optional[datetime], days: int) -> bool:
    if not dt:
        return True
    now = datetime.now(timezone.utc)
    return dt >= now - timedelta(days=days)


def require_login() -> None:
    # 簡易ログイン（SecretsでAPP_PASSWORDが未設定ならスキップ）
    app_pw = get_secret("APP_PASSWORD", "")
    if not app_pw:
        return

    if "authed" not in st.session_state:
        st.session_state.authed = False

    if st.session_state.authed:
        return

    st.warning("このアプリはパスワード保護されています。")
    pw = st.text_input("Password", type="password")
    if st.button("ログイン"):
        if pw == app_pw:
            st.session_state.authed = True
            st.success("ログインしました。")
            st.rerun()
        else:
            st.error("パスワードが違います。")
    st.stop()


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    init_db()

    require_login()

    st.title("📈 " + APP_TITLE)
    st.caption("狙い：スマホでも『銘柄/開示/要点＋数値』まで最短で見る。AI要約は押した時だけ実行。")

    gemini_api_key = get_secret("GEMINI_API_KEY", "") or get_secret("GOOGLE_API_KEY", "")
    gemini_model = get_secret("GEMINI_MODEL", "gemini-2.0-flash")
    max_pdf_bytes = int(get_secret("MAX_PDF_BYTES", "21000000"))

    can_run_ai = bool(gemini_api_key)

    if not can_run_ai:
        st.info("Gemini APIキーが未設定です。Streamlit Secrets に GEMINI_API_KEY を設定してください。")

    st.caption(f"PDF上限: {max_pdf_bytes/1_000_000:.1f}MB（Secrets の MAX_PDF_BYTES で変更可）")

    with st.expander("スクリーニング条件", expanded=True):
        colL, colM, colR = st.columns([2, 3, 2])

        with colL:
            code_input = st.text_input("銘柄コード（4桁、空なら直近全体）", value="", placeholder="例：8170")
            only_kessan = st.checkbox("決算短信だけに絞る（0件なら自動で広めに切替）", value=True)

        with colM:
            days = st.slider("直近何日を見る？", min_value=1, max_value=30, value=12)
            limit = st.slider("取得件数（大きいほど遅い）", min_value=50, max_value=500, value=300, step=50)

        with colR:
            only_pdf = st.checkbox("PDF URLがあるものだけ", value=False)
            show_ai_btn = st.checkbox("AI分析ボタンを表示", value=True)
            show_debug = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

    # ---- データ取得 ----
    items = fetch_tdnet_items(code_input.strip() if code_input else None, limit=limit)

    if show_debug:
        st.write("DEBUG: items先頭5件（title/doc_url/published/company_nameの確認）")
        st.json(items[:5])

    # ---- フィルタ ----
    # まずはユーザー条件で絞る
    filtered: list[dict[str, Any]] = []
    for it in items:
        title = it.get("title", "") or ""
        dt = it.get("published_at")
        doc_url = unwrap_tdnet_pdf(it.get("doc_url", "") or "")

        if not within_days(dt, days):
            continue
        if only_pdf and not (doc_url and is_pdf_url(doc_url)):
            continue
        if only_kessan and not is_kessan(title):
            continue

        filtered.append(it)

    # 0件なら自動で緩める（壊れない範囲で）
    relaxed_note = ""
    if only_kessan and len(filtered) == 0 and len(items) > 0:
        # 決算フィルタだけ外して再実行
        for it in items:
            dt = it.get("published_at")
            doc_url = unwrap_tdnet_pdf(it.get("doc_url", "") or "")
            if not within_days(dt, days):
                continue
            if only_pdf and not (doc_url and is_pdf_url(doc_url)):
                continue
            filtered.append(it)
        relaxed_note = "（決算短信フィルタで0件だったため、自動でフィルタを緩めて表示しています）"

    st.subheader(f"候補：{len(filtered)}件 {relaxed_note}")

    if len(filtered) == 0:
        st.info("条件に一致する開示が見つかりませんでした。日数/件数/フィルタを調整してください。")
        return

    # 表示上限（重いので最初は最大100件）
    show_n = min(len(filtered), 120)
    st.caption(f"表示：先頭 {show_n} 件（重い場合は件数を下げてください）")

    for i, it in enumerate(filtered[:show_n]):
        title = it.get("title", "") or ""
        code4 = (it.get("code") or it.get("code4") or "").strip()
        company_name = (it.get("company_name") or "").strip()
        published = it.get("published_at")
        doc_url_raw = (it.get("doc_url") or "").strip()
        doc_url = unwrap_tdnet_pdf(doc_url_raw)

        pub_str = published.isoformat() if isinstance(published, datetime) else ""

        # Streamlit DuplicateElementKey 対策：内容に依存するUID
        seed = f"{code4}|{company_name}|{pub_str}|{title}|{doc_url}|{i}"
        uid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

        # ヘッダ表示（コード横に社名）
        left = f"{code4}" if code4 else "----"
        if company_name:
            left += f"｜{company_name}"

        head = f"{left}｜{published.strftime('%Y-%m-%d %H:%M')} UTC｜{title}" if isinstance(published, datetime) else f"{left}｜{title}"

        with st.expander(head, expanded=False):
            # URL情報
            if doc_url:
                st.write(f"PDF: {doc_url}")
            else:
                st.caption("URL情報なし（AI解析不可）")

            cached = get_cached(doc_url) if doc_url else None
            status = "解析済み" if (cached and cached.get("result")) else "未解析"
            st.write(f"状態: {status}")

            cols = st.columns([1, 1, 3])

            with cols[0]:
                if st.button("キャッシュ表示", key=f"show_{uid}", disabled=not bool(doc_url)):
                    if not doc_url:
                        st.warning("PDF URLが無いためキャッシュ参照できません。")
                    else:
                        c = get_cached(doc_url)
                        if not c:
                            st.info("キャッシュはありません。")
                        else:
                            st.text_area("キャッシュ結果", c.get("result", ""), height=260)

            with cols[1]:
                # AI分析
                disabled_ai = (not show_ai_btn) or (not can_run_ai) or (not bool(doc_url)) or (not is_pdf_url(doc_url))
                btn_help = ""
                if not show_ai_btn:
                    btn_help = "（AI分析ボタン表示がOFF）"
                elif not can_run_ai:
                    btn_help = "（GEMINI_API_KEY 未設定）"
                elif not doc_url:
                    btn_help = "（PDF URLなし）"
                elif not is_pdf_url(doc_url):
                    btn_help = "（PDFではないURL）"

                if st.button(f"AI分析{btn_help}", key=f"ai_{uid}", disabled=disabled_ai):
                    # 既にキャッシュがあるならそれを出す（再解析しない）
                    c = get_cached(doc_url)
                    if c and c.get("result"):
                        st.info("キャッシュを表示します（再解析しません）。")
                        st.text_area("AI要約", c["result"], height=320)
                    else:
                        with st.spinner("PDFを取得してGeminiで要約中..."):
                            res = summarize_kessan_pdf(
                                pdf_url=doc_url,
                                gemini_api_key=gemini_api_key,
                                gemini_model=gemini_model,
                                max_pdf_bytes=max_pdf_bytes,
                            )
                        if not res.ok:
                            st.error(res.error or "解析に失敗しました。")
                        else:
                            result_text = res.text
                            set_cached(
                                pdf_url=doc_url,
                                code=code4,
                                company_name=company_name,
                                title=title,
                                published_at=pub_str,
                                result=result_text,
                            )
                            st.success("解析完了（キャッシュ保存済み）")
                            st.text_area("AI要約", result_text, height=360)

            with cols[2]:
                st.caption("※同じPDF URLはSQLiteに保存し、再解析しません（DBはキャッシュ扱い）。")


if __name__ == "__main__":
    main()
