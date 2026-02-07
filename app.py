import os
import json
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
from google import genai  # pip install google-genai

# ----------------------------
# Settings / Secrets
# ----------------------------
API_KEY = st.secrets.get("GEMINI_API_KEY", "")
APP_PASSWORD = st.secrets.get("APP_PASSWORD", "")

if not API_KEY:
    st.error("GEMINI_API_KEY が未設定です（Streamlit Secretsに設定してください）")
    st.stop()

client = genai.Client(api_key=API_KEY)

DB_PATH = "app.db"

# やのしん TDnet WEB-API（JSONが扱いやすい）
TDNET_BASE = "https://webapi.yanoshin.jp/webapi/tdnet/list"

# ----------------------------
# Auth
# ----------------------------
def require_login():
    if not APP_PASSWORD:
        st.error("APP_PASSWORD が未設定です（Secretsに設定してください）")
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
# Storage (SQLite)
# ----------------------------
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS analyses (
      doc_url TEXT PRIMARY KEY,
      code TEXT,
      title TEXT,
      published_at TEXT,
      payload_json TEXT,
      created_at TEXT
    )
    """)
    con.commit()
    con.close()

def get_cached_analysis(doc_url: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT payload_json FROM analyses WHERE doc_url=?", (doc_url,))
    row = cur.fetchone()
    con.close()
    if row:
        return json.loads(row[0])
    return None

def save_analysis(doc_url, code, title, published_at, payload: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
      INSERT OR REPLACE INTO analyses(doc_url, code, title, published_at, payload_json, created_at)
      VALUES(?,?,?,?,?,?)
    """, (doc_url, code, title, published_at, json.dumps(payload, ensure_ascii=False), datetime.now().isoformat()))
    con.commit()
    con.close()

# ----------------------------
# TDnet Fetch
# ----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def fetch_tdnet_list_json(code: str | None, days: int = 3, has_xbrl: bool = False, limit: int = 200):
    """
    指定銘柄コード、またはrecentから直近を取得。
    daysは取得の“視野”で、アプリ側でさらにフィルタする前提（壊れにくい）。
    """
    if code and code.isdigit() and len(code) == 4:
        # 銘柄別（Atom/RSSもあるがJSONが扱いやすい）
        url = f"{TDNET_BASE}/{code}.json?limit={limit}"
    else:
        url = f"{TDNET_BASE}/recent.json?limit={limit}"

    if has_xbrl:
        url += "&hasXBRL=1"

    r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    data = r.json()

    # dataの形はAPI側の仕様に依存するので、itemsを“それっぽく”正規化して返す
    items = []
    for it in data.get("items", []):
        td = it.get("TDnet") or it  # json/json2差分対策
        title = td.get("title", "")
        doc_url = td.get("document_url", "")  # hasXBRL=1ならXBRLへのリンクになり得る点に注意 :contentReference[oaicite:10]{index=10}
        code_ = str(td.get("code", "")) if td.get("code") else ""
        published = td.get("published_at") or td.get("pubdate") or td.get("date") or ""

        items.append({
            "code": code_,
            "title": title,
            "doc_url": doc_url,
            "published": published,
        })

    # 直近days日以内っぽいものだけ（publishedが取れない場合は残す）
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    filtered = []
    for x in items:
        p = x["published"]
        if not p:
            filtered.append(x)
            continue
        try:
            dt = datetime.fromisoformat(p.replace("Z", "+00:00"))
            if dt >= cutoff:
                filtered.append(x)
        except Exception:
            filtered.append(x)

    return filtered

# ----------------------------
# PDF Download
# ----------------------------
def download_to_temp(url: str) -> str:
    r = requests.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.write(r.content)
    tmp.close()
    return tmp.name

# ----------------------------
# Gemini JSON Extraction
# ----------------------------
def analyze_doc_with_gemini(doc_url: str) -> dict:
    """
    決算短信PDF（またはPDFリンク）を読み、可視化できるJSONを返す。
    """
    cached = get_cached_analysis(doc_url)
    if cached:
        return cached

    pdf_path = None
    try:
        pdf_path = download_to_temp(doc_url)

        uploaded = client.files.upload(file=pdf_path)

        # “可視化可能なJSON”に固定するのがポイント
        prompt = """
あなたは日本株の決算短信を投資家目線で分析するアナリストです。
添付PDF（決算短信）から、次のJSONだけを出力してください（説明文は禁止）。
数値が見つからない場合は null にしてください。単位は可能なら「百万円」「円」などを明記。

JSONスキーマ（厳守）:
{
  "summary_1min": "string",
  "headline": {
    "tone": "強気|中立|弱気|不明",
    "score_0_10": number
  },
  "performance": {
    "period": "例: 2025年度3Q など",
    "sales_yoy_pct": number|null,
    "op_yoy_pct": number|null,
    "ordinary_yoy_pct": number|null,
    "net_yoy_pct": number|null
  },
  "guidance": {
    "raised": true|false|null,
    "lowered": true|false|null,
    "unchanged": true|false|null,
    "sales_full_year": number|null,
    "op_full_year": number|null,
    "eps_full_year": number|null
  },
  "drivers": {
    "profit_up_reasons": ["string", "..."],
    "profit_down_reasons": ["string", "..."]
  },
  "risks": {
    "short_term": ["string", "..."],
    "mid_term": ["string", "..."]
  },
  "watch_points": ["string", "..."]
}
"""

        resp = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=[prompt, uploaded],
            # ここはSDK側の挙動差があるので、壊れにくくするため「JSONを返せ」とプロンプトで縛っておく
        )

        text = (resp.text or "").strip()

        # 最低限のJSONパース（壊れたらエラー表示）
        payload = json.loads(text)
        return payload

    finally:
        if pdf_path and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except Exception:
                pass

# ----------------------------
# UI
# ----------------------------
init_db()
require_login()

st.title("📈 決算短信スクリーニング & ビジュアライズ")

with st.expander("スクリーニング条件", expanded=True):
    code = st.text_input("銘柄コード（4桁、空なら全体の直近）", value="")
    days = st.slider("直近何日を見る？", 1, 14, 3)
    only_xbrl = st.checkbox("XBRLがある開示だけ（見つかる率↑）", value=False)
    limit = st.slider("取得件数（重いほど遅い）", 50, 300, 200)

items = fetch_tdnet_list_json(code.strip() or None, days=days, has_xbrl=only_xbrl, limit=limit)

# 決算短信っぽいものだけ
kessan = [x for x in items if "決算短信" in (x["title"] or "")]
st.subheader(f"候補：{len(kessan)}件（決算短信のみ）")

if not kessan:
    st.info("条件に一致する決算短信が見つかりませんでした。")
    st.stop()

for x in kessan[:50]:
    title = x["title"]
    doc_url = x["doc_url"]
    code_ = x["code"]
    published = x["published"]

    with st.container(border=True):
        st.write(f"**{code_}**  {title}")
        if published:
            st.caption(f"公開: {published}")
        st.caption(doc_url)

        colA, colB = st.columns([1, 2])
        with colA:
            run = st.button("分析して表示", key=doc_url)
        with colB:
            st.caption("※同じURLはDBキャッシュします（再解析しません）")

        if run:
            with st.spinner("解析中（初回は数十秒かかることがあります）"):
                payload = analyze_doc_with_gemini(doc_url)

            # 保存
            save_analysis(doc_url, code_, title, published, payload)

            # 可視化（最低限）
            st.markdown("### 1分要約")
            st.write(payload.get("summary_1min", ""))

            st.markdown("### トーン / スコア")
            headline = payload.get("headline", {})
            st.write(f"トーン: {headline.get('tone')} / スコア: {headline.get('score_0_10')}")

            st.markdown("### 前年比（%）")
            perf = payload.get("performance", {})
            chart_data = {
                "sales_yoy_pct": perf.get("sales_yoy_pct"),
                "op_yoy_pct": perf.get("op_yoy_pct"),
                "ordinary_yoy_pct": perf.get("ordinary_yoy_pct"),
                "net_yoy_pct": perf.get("net_yoy_pct"),
            }
            # 数値だけ抽出して棒グラフ
            numeric = {k: v for k, v in chart_data.items() if isinstance(v, (int, float))}
            if numeric:
                st.bar_chart(numeric)
            else:
                st.info("前年比の数値を抽出できませんでした（PDFの書式差の可能性）。")

            st.markdown("### 増減益理由 / リスク")
            drivers = payload.get("drivers", {})
            st.write("増益理由:", drivers.get("profit_up_reasons", []))
            st.write("減益理由:", drivers.get("profit_down_reasons", []))

            risks = payload.get("risks", {})
            st.write("短期リスク:", risks.get("short_term", []))
            st.write("中期リスク:", risks.get("mid_term", []))

st.divider()
st.subheader("手動（PDF URLを貼って解析）")
manual = st.text_input("決算短信PDFのURL")
if st.button("手動解析") and manual:
    with st.spinner("解析中..."):
        payload = analyze_doc_with_gemini(manual)
    st.json(payload)
