from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
import streamlit as st

from src.tdnet import fetch_tdnet_items


APP_TITLE = "決算短信スクリーニング＆ビジュアライズ"
DB_PATH = "app.db"

# 決算っぽいタイトル判定（雑に広め）
_KESSAN_RE = re.compile(r"(決算短信|四半期決算|通期決算|Financial Results|Earnings)", re.IGNORECASE)


def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_dt_utc(dt: Optional[datetime]) -> str:
    if not dt:
        return "----"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_cache (
                doc_url TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def db_get(doc_url: str) -> Optional[dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute("SELECT payload FROM analysis_cache WHERE doc_url = ?", (doc_url,))
        row = cur.fetchone()
        if not row:
            return None
        return json.loads(row[0])
    except Exception:
        return None
    finally:
        conn.close()


def db_set(doc_url: str, payload: dict[str, Any]) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO analysis_cache (doc_url, created_at, payload) VALUES (?, ?, ?)",
            (doc_url, now_utc().isoformat(), json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()


def is_allowed_pdf_url(url: str) -> bool:
    """
    AI解析に回すURLだけは安全側に寄せる。
    表示自体は制限を緩くしてもOKだが、AI解析は tdnet のPDFだけに。
    """
    u = (url or "").strip()
    if not u:
        return False
    # yanoshinのrd.phpでラップされてても、中身がrelease.tdnet.infoならOK
    return ("release.tdnet.info" in u) and u.lower().endswith(".pdf")


def download_pdf_bytes(url: str, max_bytes: int) -> bytes:
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}, stream=True)
    r.raise_for_status()

    data = bytearray()
    for chunk in r.iter_content(chunk_size=1024 * 64):
        if not chunk:
            continue
        data.extend(chunk)
        if len(data) > max_bytes:
            raise ValueError(f"PDF too large: {len(data)} bytes (limit {max_bytes})")
    return bytes(data)


def analyze_pdf_with_openai(pdf_bytes: bytes, doc_url: str) -> dict[str, Any]:
    """
    既存の src/analyzer.py がある前提なら、そこへ寄せたいが、
    ここでは「壊れない」最優先でスタブ + 既存関数があれば使う。
    """
    # 既存 analyzer があるならそれを使う（関数名違いでも落ちないようtry）
    try:
        from src.analyzer import analyze_pdf_bytes  # type: ignore
        return analyze_pdf_bytes(pdf_bytes, source_url=doc_url)  # type: ignore
    except Exception:
        pass

    # analyzer が無い/壊れててもアプリ自体は落とさない
    return {
        "error": "analyzer 未設定（src/analyzer.py の analyze_pdf_bytes を用意してください）",
        "source_url": doc_url,
    }


def get_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    try:
        v = st.secrets.get(name)  # type: ignore[attr-defined]
        if v is None:
            return default
        return str(v)
    except Exception:
        return default


def require_login() -> bool:
    """
    APP_PASSWORD が secrets にある場合だけログイン必須にする。
    未設定ならログインなしで通す（開発用）。
    """
    app_pw = get_secret("APP_PASSWORD", "")
    if not app_pw:
        st.info("APP_PASSWORD が未設定のため、ログイン無しで表示中（Secrets に設定すると有効化されます）")
        st.session_state["authed"] = True
        return True

    if st.session_state.get("authed") is True:
        return True

    st.warning("ログインが必要です。")
    with st.form("login_form"):
        pw = st.text_input("パスワード", type="password")
        ok = st.form_submit_button("ログイン")
        if ok:
            if pw == app_pw:
                st.session_state["authed"] = True
                st.success("ログインしました。")
                st.rerun()
            else:
                st.error("パスワードが違います。")
    return False


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("狙い：スマホでも「銘柄→開示→要点＋数値」まで最短で見る。AI要約は押した時だけ実行。")

    init_db()

    # Secrets: OpenAI key（必要なら）
    openai_key = get_secret("OPENAI_API_KEY", "")
    can_run_ai = bool(openai_key)

    # PDFサイズ上限（Secretsにあればそれを使用）
    max_pdf_bytes = int(get_secret("MAX_PDF_BYTES", "20000000") or "20000000")  # default 20MB
    st.caption(f"PDF上限: {max_pdf_bytes/1_000_000:.1f}MB（Secrets の MAX_PDF_BYTES で変更可）")

    if not require_login():
        return

    with st.expander("スクリーニング条件", expanded=True):
        colL, colM, colR = st.columns([2, 2, 2])

        with colL:
            code4 = st.text_input("銘柄コード（4桁、空なら直近全体）", value="", placeholder="例：8170")
            kessan_only = st.checkbox("決算短信だけに絞る（0件なら自動で広めに切替）", value=True)

        with colM:
            days = st.slider("直近何日を見る？", min_value=1, max_value=30, value=12)
            limit = st.slider("取得件数（大きいほど遅い）", min_value=50, max_value=500, value=300, step=10)

        with colR:
            pdf_only = st.checkbox("PDF URLがあるものだけ", value=False)
            show_ai_button = st.checkbox("AI分析ボタンを表示", value=True)
            debug_json = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

    # --- fetch ---
    items = fetch_tdnet_items(code4.strip() or None, limit=limit)

    # DEBUG: raw確認
    if debug_json:
        st.markdown("### DEBUG: items先頭5件（title/doc_url/published/company_name の確認）")
        preview = []
        for it in items[:5]:
            preview.append(
                {
                    "title": it.get("title", ""),
                    "code4": it.get("code4", ""),
                    "company_code": it.get("company_code", ""),
                    "company_name": it.get("company_name", ""),
                    "doc_url": (it.get("doc_url") or "")[:140],
                    "published_at": (it.get("published_at").isoformat() if it.get("published_at") else None),
                    "raw_keys": list((it.get("raw") or {}).keys())[:20],
                }
            )
        st.json(preview, expanded=False)

    # --- filter ---
    cutoff = now_utc() - timedelta(days=days)

    filtered: list[dict[str, Any]] = []
    for it in items:
        title = it.get("title", "") or ""
        doc_url = (it.get("doc_url") or "").strip()
        published: Optional[datetime] = it.get("published_at")

        # 日数フィルタ：published_at が取れてるものはきっちり、取れてないものは「落とさず残す」
        if published is not None and published < cutoff:
            continue

        if pdf_only and not doc_url:
            continue

        if kessan_only and not is_kessan(title):
            continue

        filtered.append(it)

    # 0件なら自動で緩める（kessanだけ外す）
    auto_relaxed = False
    if kessan_only and len(filtered) == 0:
        auto_relaxed = True
        for it in items:
            doc_url = (it.get("doc_url") or "").strip()
            published: Optional[datetime] = it.get("published_at")

            if published is not None and published < cutoff:
                continue
            if pdf_only and not doc_url:
                continue
            filtered.append(it)

    # 件数表示
    st.subheader(f"候補：{len(filtered)}件" + ("（決算フィルタ自動解除）" if auto_relaxed else ""))

    if len(filtered) == 0:
        st.info("条件に一致する開示が見つかりませんでした。日数/件数/フィルタを調整してください。")
        return

    # --- render list ---
    # 解析用：同じURLはSQLiteに保存して再解析しない
    for i, it in enumerate(filtered[:100]):
        title = it.get("title", "") or ""
        code4_ = it.get("code4", "") or ""
        company_code = it.get("company_code", "") or ""
        company_name = it.get("company_name", "") or ""
        doc_url = (it.get("doc_url") or "").strip()
        published: Optional[datetime] = it.get("published_at")

        # Streamlit widget key 衝突回避用 uid
        seed = f"{company_code}|{published.isoformat() if published else ''}|{title}|{doc_url}|{i}"
        uid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

        # 表示行：コード横に社名を出す
        # 例: 8170(68170)｜社名｜2026-02-06 17:00 UTC｜タイトル
        left_code = f"{code4_}({company_code})" if code4_ or company_code else "----"
        head = f"{left_code}｜{company_name or '社名不明'}｜{fmt_dt_utc(published)}｜{title}"

        with st.expander(head, expanded=False):
            cols = st.columns([1, 1, 2])

            # URL表示
            if doc_url:
                st.markdown(f"**PDF:** {doc_url}")
            else:
                st.caption("URL情報なし（AI解析不可）")

            # キャッシュの有無
            cached = db_get(doc_url) if doc_url else None
            st.write("**状態:**", "解析済み" if cached else "未解析")

            with cols[0]:
                if st.button("キャッシュ表示", key=f"show_{uid}", disabled=not bool(cached)):
                    st.json(cached, expanded=True)

            with cols[1]:
                # AI分析できる条件
                allowed = bool(doc_url) and is_allowed_pdf_url(doc_url) and can_run_ai and show_ai_button
                if st.button("AI分析", key=f"ai_{uid}", disabled=not allowed):
                    # 既にキャッシュがあるならそれを出す（再解析しない）
                    cached2 = db_get(doc_url)
                    if cached2:
                        st.success("既に解析済み（キャッシュから表示）")
                        st.json(cached2, expanded=True)
                    else:
                        try:
                            with st.spinner("PDF取得中…"):
                                pdf_bytes = download_pdf_bytes(doc_url, max_bytes=max_pdf_bytes)
                            with st.spinner("AI解析中…"):
                                payload = analyze_pdf_with_openai(pdf_bytes, doc_url)
                            db_set(doc_url, payload)
                            st.success("解析しました（キャッシュ保存済み）")
                            st.json(payload, expanded=True)
                        except Exception as e:
                            st.error(f"解析に失敗: {e}")

            with cols[2]:
                # 軽い補助表示
                st.caption("※同じPDF URLはSQLiteに保存し、再解析しません（DBはキャッシュ扱い）。")
                if doc_url and not is_allowed_pdf_url(doc_url):
                    st.warning("このURLはAI解析対象外（release.tdnet.info のPDFのみ解析）")

    if len(filtered) > 100:
        st.info("表示は先頭100件まで。条件を絞り込んでください。")


if __name__ == "__main__":
    main()
