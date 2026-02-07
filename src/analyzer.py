from __future__ import annotations

import json
import os
import re
import tempfile
from typing import Any, Dict
from urllib.parse import urlparse

import requests
import streamlit as st
from google import genai


# ----------------------------
# Strict security settings
# ----------------------------
MAX_PDF_BYTES_DEFAULT = 20 * 1024 * 1024  # 20MB
ALLOWED_HOST_SUFFIXES = ("release.tdnet.info",)


def ai_is_enabled() -> bool:
    return bool(st.secrets.get("GEMINI_API_KEY", ""))


def _client() -> genai.Client:
    api_key = st.secrets.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")
    return genai.Client(api_key=api_key)


def _is_allowed_pdf_url(url: str) -> bool:
    try:
        u = urlparse(url)
        if u.scheme not in ("http", "https"):
            return False
        host = (u.hostname or "").lower()
        if not host:
            return False
        if not any(host == s or host.endswith("." + s) for s in ALLOWED_HOST_SUFFIXES):
            return False
        if not u.path.lower().endswith(".pdf"):
            return False
        return True
    except Exception:
        return False


def _download_to_temp(url: str) -> str:
    """
    安全寄りにPDFをダウンロード:
    - allowlist host
    - Content-Type / 拡張子チェック
    - サイズ上限
    """
    if not _is_allowed_pdf_url(url):
        raise ValueError("許可されていないURLです（TDnet公式PDFのみ）")

    max_bytes = int(st.secrets.get("MAX_PDF_BYTES", MAX_PDF_BYTES_DEFAULT))

    # まずHEADでサイズを見る（サーバが対応しない場合もある）
    try:
        h = requests.head(url, timeout=15, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        if h.ok:
            cl = h.headers.get("Content-Length")
            if cl and cl.isdigit() and int(cl) > max_bytes:
                raise ValueError(f"PDFが大きすぎます（{int(cl)} bytes > {max_bytes} bytes）")
    except requests.RequestException:
        # HEAD失敗は無視（GETでチェック）
        pass

    r = requests.get(
        url,
        timeout=30,
        allow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0"},
        stream=True,
    )
    r.raise_for_status()

    ctype = (r.headers.get("Content-Type") or "").lower()
    # ここも強めに（octet-streamがあるので完全には縛らないが、text/htmlは弾く）
    if "text/html" in ctype:
        raise ValueError(f"PDFではない可能性があります: content-type={ctype}")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    size = 0
    for chunk in r.iter_content(chunk_size=1024 * 256):
        if not chunk:
            continue
        size += len(chunk)
        if size > max_bytes:
            tmp.close()
            try:
                os.remove(tmp.name)
            except Exception:
                pass
            raise ValueError(f"PDFが大きすぎます（>{max_bytes} bytes）。")
        tmp.write(chunk)

    tmp.close()
    return tmp.name


# ----------------------------
# JSON extraction helpers
# ----------------------------
def _strip_code_fences(text: str) -> str:
    t = text.strip()
    # ```json ... ```
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def _extract_first_json_object(text: str) -> str:
    """
    文章が混ざっても最初の{...}を抜く（簡易）。
    """
    t = text
    start = t.find("{")
    end = t.rfind("}")
    if start != -1 and end != -1 and end > start:
        return t[start : end + 1]
    return text


def _ensure_schema(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    UIが壊れない最低限のキーを補完。
    """
    payload.setdefault("summary_1min", "")
    payload.setdefault("headline", {})
    payload["headline"].setdefault("tone", "不明")
    payload["headline"].setdefault("score_0_10", 0)

    payload.setdefault("performance", {})
    payload["performance"].setdefault("period", "")
    for k in ["sales_yoy_pct", "op_yoy_pct", "ordinary_yoy_pct", "net_yoy_pct"]:
        payload["performance"].setdefault(k, None)

    payload.setdefault("guidance", {})
    for k in ["raised", "lowered", "unchanged", "sales_full_year", "op_full_year", "eps_full_year"]:
        payload["guidance"].setdefault(k, None)

    payload.setdefault("drivers", {})
    payload["drivers"].setdefault("profit_up_reasons", [])
    payload["drivers"].setdefault("profit_down_reasons", [])

    payload.setdefault("risks", {})
    payload["risks"].setdefault("short_term", [])
    payload["risks"].setdefault("mid_term", [])

    payload.setdefault("watch_points", [])
    return payload


def analyze_pdf_to_json(pdf_url: str) -> dict:
    """
    決算短信PDFを解析して可視化向けJSONを返す（制限強め）。
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
        text = _strip_code_fences(text)
        text = _extract_first_json_object(text)

        # 1回目パース
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            # 復旧：JSON部分だけをもう少し強引に抽出し直す
            cleaned = _extract_first_json_object(text)
            cleaned = _strip_code_fences(cleaned)
            payload = json.loads(cleaned)

        if not isinstance(payload, dict):
            raise ValueError("AI output is not a JSON object")

        return _ensure_schema(payload)

    finally:
        if pdf_path and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except Exception:
                pass
