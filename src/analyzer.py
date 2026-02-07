from __future__ import annotations

import json
import os
import tempfile

import requests
import streamlit as st
from google import genai


def ai_is_enabled() -> bool:
    return bool(st.secrets.get("GEMINI_API_KEY", ""))


def _client() -> genai.Client:
    api_key = st.secrets.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")
    return genai.Client(api_key=api_key)


def _download_to_temp(url: str) -> str:
    r = requests.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()
    # 厳密にはしないが、怪しい場合は弾く（HTMLを食わせる事故を減らす）
    if ("pdf" not in ctype) and (not url.lower().endswith(".pdf")):
        raise ValueError(f"PDFではない可能性があります: content-type={ctype}")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.write(r.content)
    tmp.close()
    return tmp.name


def analyze_pdf_to_json(pdf_url: str) -> dict:
    """
    決算短信PDFを解析して可視化向けJSONを返す。
    """
    client = _client()
    pdf_path = None
    try:
        pdf_path = _download_to_temp(pdf_url)
        uploaded = client.files.upload(file=pdf_path)

        prompt = """
あなたは日本株の決算短信を投資家目線で分析するアナリストです。
添付PDF（決算短信）から、次のJSONだけを出力してください（説明文は禁止）。
数値が見つからない場合は null にしてください。

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
        )

        text = (resp.text or "").strip()

        # よくある事故：```json ... ``` を吐く場合があるので剥がす
        if text.startswith("```"):
            text = text.strip("`")
            # 先頭のjson等を雑に除去
            text = text.replace("json", "", 1).strip()

        payload = json.loads(text)

        # 最低限の形チェック（壊れづらくする）
        if not isinstance(payload, dict):
            raise ValueError("AI output is not a JSON object")

        return payload

    finally:
        if pdf_path and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except Exception:
                pass
