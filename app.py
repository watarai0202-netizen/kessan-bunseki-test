import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
import streamlit as st

from src.tdnet import fetch_tdnet_items  # ← src/tdnet.py を使う


APP_TITLE = "決算短信スクリーニング＆ビジュアライズ"
DB_PATH = "app.db"

# 「決算っぽい」タイトル判定（ゆるめ）
_KESSAN_RE = re.compile(
    r"(決算短信|四半期決算|通期決算|決算説明会|Financial Results|Earnings)",
    re.IGNORECASE,
)


def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))


def init_db() -> None:
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_cache (
                pdf_url TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                model TEXT,
                result_md TEXT NOT NULL
            )
            """
        )
        con.commit()
    finally:
        con.close()


def get_cached_analysis(pdf_url: str) -> Optional[dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute(
            "SELECT pdf_url, created_at, model, result_md FROM analysis_cache WHERE pdf_url = ?",
            (pdf_url,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"pdf_url": row[0], "created_at": row[1], "model": row[2], "result_md": row[3]}
    finally:
        con.close()


def set_cached_analysis(pdf_url: str, model: str, result_md: str) -> None:
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            """
            INSERT INTO analysis_cache (pdf_url, created_at, model, result_md)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(pdf_url) DO UPDATE SET
              created_at=excluded.created_at,
              model=excluded.model,
              result_md=excluded.result_md
            """
            ,
            (pdf_url, datetime.now(timezone.utc).isoformat(), model, result_md),
        )
        con.commit()
    finally:
        con.close()


def extract_release_tdnet_pdf(url: str) -> str:
    """
    yanoshin の rd.php 形式（.../rd.php?https://www.release.tdnet.info/...pdf）
    を release.tdnet.info の直URLに寄せる（あれば）。
    """
    u = (url or "").strip()
    if not u:
        return ""
    # すでにrelease.tdnetならそのまま
    if "release.tdnet.info" in u and u.lower().endswith(".pdf"):
        return u
    # rd.php?https://... を雑に抜く
    m = re.search(r"(https?://[^ \n\r\t]+\.pdf)", u)
    if m:
        return m.group(1)
    return u


def download_pdf_bytes(pdf_url: str, max_bytes: int) -> bytes:
    # streamlit cloud / requestsの相性でUA入れる
    r = requests.get(pdf_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}, stream=True)
    r.raise_for_status()

    chunks = []
    total = 0
    for chunk in r.iter_content(chunk_size=1024 * 256):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(f"PDFが上限を超えました: {total/1024/1024:.1f}MB > {max_bytes/1024/1024:.1f}MB")
        chunks.append(chunk)
    return b"".join(chunks)


def pdf_to_text(pdf_bytes: bytes) -> str:
    """
    依存を増やしにくいように pypdf を優先して使う。
    requirements.txt に 'pypdf' を入れてください。
    """
    try:
        from pypdf import PdfReader
    except Exception as e:
        raise RuntimeError("pypdf が入っていません。requirements.txt に pypdf を追加してください。") from e

    import io

    reader = PdfReader(io.BytesIO(pdf_bytes))
    texts = []
    for page in reader.pages[:25]:  # 取り過ぎると重いので上限（必要なら増やせる）
        t = page.extract_text() or ""
        t = t.strip()
        if t:
            texts.append(t)
    return "\n\n".join(texts).strip()


def gemini_summarize(text: str, *, api_key: str, model: str) -> str:
    """
    Gemini API（google-generativeai）で要約。
    requirements.txt に 'google-generativeai' を入れてください。
    """
    try:
        import google.generativeai as genai
    except Exception as e:
        raise RuntimeError("google-generativeai が入っていません。requirements.txt に google-generativeai を追加してください。") from e

    genai.configure(api_key=api_key)
    m = genai.GenerativeModel(model)

    prompt = f"""
あなたは日本株の決算短信を読むプロのアナリストです。
以下のテキストはTDnet開示PDFから抽出したものです。誤抽出や欠落があり得ます。

目的：
- 「株価インパクト」を最短で判断できる要点に圧縮
- 数値がある場合は、前年差・進捗・通期見通しの方向性を重視

出力はMarkdownで、次の見出しを必ず含めてください：
## 3行まとめ
## 業績ハイライト（売上・利益・進捗）
## ガイダンス/見通し（上方/据置/下方の有無）
## ポジ要因 / ネガ要因
## 次に確認すべき一次情報（短信のどの章、補足資料など）

--- 対象テキスト ---
{text[:120000]}
"""

    resp = m.generate_content(prompt)
    out = (getattr(resp, "text", None) or "").strip()
    if not out:
        out = "（Geminiの応答が空でした）"
    return out


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("狙い：スマホでも「銘柄/開示/要点＋数値」まで最短で見る。AI要約は押した時だけ実行。")

    # Secrets
    gemini_key = (st.secrets.get("GEMINI_API_KEY", "") or "").strip()
    gemini_model = (st.secrets.get("GEMINI_MODEL", "") or "").strip() or "gemini-1.5-flash"
    max_pdf_bytes = int(st.secrets.get("MAX_PDF_BYTES", 21 * 1024 * 1024))

    st.text(f"PDF上限: {max_pdf_bytes/1024/1024:.1f}MB（Secrets の MAX_PDF_BYTES で変更可）")

    init_db()

    # --- UI ---
    with st.expander("スクリーニング条件", expanded=True):
        colL, colC, colR = st.columns([2.2, 3.6, 2.2])

        with colL:
            code = st.text_input("銘柄コード（4桁、空なら直近全体）", value="", placeholder="例：8170")
            only_kessan = st.checkbox("決算短信だけに絞る（0件なら自動で広めに切替）", value=True)

        with colC:
            days = st.slider("直近何日を見る？", min_value=1, max_value=30, value=12)
            limit = st.slider("取得件数（大きいほど遅い）", min_value=50, max_value=500, value=300, step=10)

        with colR:
            pdf_only = st.checkbox("PDF URLがあるものだけ", value=False)
            show_ai = st.checkbox("AI分析ボタンを表示", value=True)
            debug = st.checkbox("DEBUG表示（先頭5件のJSON）", value=False)

    # --- Fetch ---
    try:
        items = fetch_tdnet_items(code if code.strip() else None, limit=limit)
    except Exception as e:
        st.error(f"TDnet取得に失敗: {e}")
        st.stop()

    # --- Filter by days ---
    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=days)

    def in_range(it: dict[str, Any]) -> bool:
        dt = it.get("published_at")
        return (dt is None) or (dt >= cutoff)

    items_in_range = [it for it in items if in_range(it)]

    # --- PDF filter ---
    if pdf_only:
        items_in_range = [it for it in items_in_range if (it.get("doc_url") or "").strip()]

    # --- Kessan filter (auto relax) ---
    filtered = items_in_range
    relaxed = False
    if only_kessan:
        k = [it for it in items_in_range if is_kessan(it.get("title", ""))]
        if len(k) == 0:
            filtered = items_in_range
            relaxed = True
        else:
            filtered = k

    # 表示は重くなるので上限
    show_n = 120
    st.subheader(
        f"候補：{len(filtered)}件"
        + ("（決算短信フィルタで0件だったため、自動でフィルタを緩めて表示しています）" if relaxed else "")
    )
    st.caption(f"表示：先頭 {min(show_n, len(filtered))}件（重い場合は取得件数を下げてください）")

    if debug:
        st.markdown("**DEBUG: items先頭5件（title/doc_url/published/company_nameの確認）**")
        st.json(
            [
                {
                    "title": it.get("title", ""),
                    "company_name": it.get("company_name", ""),
                    "company_code": it.get("company_code", ""),
                    "code4": it.get("code4", ""),
                    "doc_url": it.get("doc_url", ""),
                    "published_at": (it.get("published_at").isoformat() if it.get("published_at") else None),
                    "raw_has_keys": list((it.get("raw") or {}).keys())[:10],
                }
                for it in filtered[:5]
            ]
        )

    if len(filtered) == 0:
        st.info("条件に一致する開示が見つかりませんでした。日数/件数/フィルタを調整してください。")
        st.stop()

    # --- AI runnable? ---
    can_run_ai = bool(gemini_key)

    if show_ai and not can_run_ai:
        st.warning("GEMINI_API_KEY が未設定のため、AI分析は無効です（StreamlitのSecretsに設定してください）。")

    # --- Render list ---
    for i, it in enumerate(filtered[:show_n]):
        title = (it.get("title") or "").strip()
        company_name = (it.get("company_name") or "").strip()
        code4 = (it.get("code4") or "").strip()
        company_code = (it.get("company_code") or "").strip()

        published = it.get("published_at")
        published_s = published.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if published else "日時不明"

        doc_url_raw = (it.get("doc_url") or "").strip()
        doc_url = extract_release_tdnet_pdf(doc_url_raw)

        # UID for streamlit key collision prevention
        seed = f"{company_code}|{published_s}|{title}|{doc_url}|{i}"
        uid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]

        # 見出し：コード + 社名 + 日時 + タイトル
        left = code4 if code4 else "----"
        mid = company_name if company_name else "不明"
        head = f"{left} | {mid} | {published_s} | {title if title else '（タイトル不明）'}"

        with st.expander(head, expanded=False):
            if doc_url:
                st.markdown(f"PDF: {doc_url}")
            else:
                st.caption("URL情報なし（AI解析不可）")

            cache = get_cached_analysis(doc_url) if doc_url else None
            if cache:
                st.success(f"状態: 解析済み（{cache['created_at']} / model={cache.get('model') or '-'}）")
            else:
                st.info("状態: 未解析")

            cols = st.columns([1, 1, 4])
            with cols[0]:
                if st.button("キャッシュ表示", key=f"show_{uid}", disabled=not bool(cache)):
                    st.markdown(cache["result_md"] if cache else "（キャッシュなし）")

            with cols[1]:
                btn_label = "AI分析"
                disabled_reason = (not can_run_ai) or (not doc_url)
                if st.button(btn_label, key=f"ai_{uid}", disabled=disabled_reason):
                    # 既にキャッシュあればそれを表示
                    cache2 = get_cached_analysis(doc_url) if doc_url else None
                    if cache2:
                        st.markdown(cache2["result_md"])
                    else:
                        with st.spinner("PDF取得→抽出→Gemini要約中…"):
                            try:
                                pdf_bytes = download_pdf_bytes(doc_url, max_bytes=max_pdf_bytes)
                                text = pdf_to_text(pdf_bytes)
                                if not text:
                                    raise RuntimeError("PDFからテキストが抽出できませんでした（画像PDFの可能性）。")
                                md = gemini_summarize(text, api_key=gemini_key, model=gemini_model)
                                set_cached_analysis(doc_url, gemini_model, md)
                                st.markdown(md)
                            except Exception as e:
                                st.error(f"AI分析に失敗: {e}")

            with cols[2]:
                st.caption("※同じPDF URLはSQLiteに保存し、再解析しません（DBはキャッシュ扱い）。")


if __name__ == "__main__":
    main()
