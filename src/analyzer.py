from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

import requests

# PDF抽出（requirementsに pypdf を入れてね）
try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None  # type: ignore


@dataclass
class AnalyzeResult:
    ok: bool
    text: str
    error: str = ""
    tokens: Optional[int] = None


def extract_text_from_pdf_bytes(pdf_bytes: bytes, max_pages: int = 30) -> str:
    if PdfReader is None:
        return "PDF抽出ライブラリ(pypdf)が未インストールです。requirements に `pypdf` を追加してください。"

    try:
        reader = PdfReader(io_bytes := _bytes_to_filelike(pdf_bytes))
        texts: list[str] = []
        pages = reader.pages[:max_pages]
        for p in pages:
            t = p.extract_text() or ""
            if t.strip():
                texts.append(t)
        return "\n\n".join(texts).strip()
    except Exception as e:
        return f"PDF抽出に失敗しました: {e}"


def _bytes_to_filelike(b: bytes):
    import io
    return io.BytesIO(b)


def download_pdf(url: str, max_bytes: int) -> tuple[bytes | None, str]:
    """
    PDFをダウンロード。サイズ上限を超えたら止める。
    """
    u = (url or "").strip()
    if not u:
        return None, "PDF URLが空です。"

    try:
        with requests.get(u, stream=True, timeout=35, headers={"User-Agent": "Mozilla/5.0"}) as r:
            r.raise_for_status()

            total = 0
            chunks: list[bytes] = []
            for chunk in r.iter_content(chunk_size=1024 * 128):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_bytes:
                    return None, f"PDFサイズが上限を超えました（>{max_bytes} bytes）"
                chunks.append(chunk)

            return b"".join(chunks), ""
    except Exception as e:
        return None, f"PDFダウンロード失敗: {e}"


def gemini_generate(
    api_key: str,
    model: str,
    prompt: str,
    temperature: float = 0.2,
) -> AnalyzeResult:
    """
    Gemini API（Generative Language API）をRESTで呼ぶ
    """
    api_key = (api_key or "").strip()
    if not api_key:
        return AnalyzeResult(ok=False, text="", error="GEMINI_API_KEY が未設定です。")

    model = (model or "").strip() or "gemini-2.0-flash"

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"

    payload: dict[str, Any] = {
        "contents": [
            {"role": "user", "parts": [{"text": prompt}]}
        ],
        "generationConfig": {
            "temperature": float(temperature),
        },
    }

    try:
        r = requests.post(endpoint, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()

        # candidates[0].content.parts[0].text
        candidates = data.get("candidates") or []
        if not candidates:
            return AnalyzeResult(ok=False, text="", error=f"Geminiの返答が空です: {json.dumps(data)[:500]}")

        parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
        if not parts:
            return AnalyzeResult(ok=False, text="", error=f"Gemini partsが空です: {json.dumps(data)[:500]}")

        text = (parts[0] or {}).get("text") or ""
        if not str(text).strip():
            return AnalyzeResult(ok=False, text="", error="Geminiの返答テキストが空です。")

        return AnalyzeResult(ok=True, text=str(text).strip())
    except Exception as e:
        return AnalyzeResult(ok=False, text="", error=f"Gemini呼び出し失敗: {e}")


def summarize_kessan_pdf(
    pdf_url: str,
    gemini_api_key: str,
    gemini_model: str,
    max_pdf_bytes: int,
) -> AnalyzeResult:
    pdf_bytes, err = download_pdf(pdf_url, max_bytes=max_pdf_bytes)
    if pdf_bytes is None:
        return AnalyzeResult(ok=False, text="", error=err)

    text = extract_text_from_pdf_bytes(pdf_bytes, max_pages=35)
    if not text or "失敗" in text[:50] or "未インストール" in text[:100]:
        # 抽出失敗の可能性が高い場合はそのまま返す
        if "PDF抽出" in text or "未インストール" in text or "失敗" in text:
            return AnalyzeResult(ok=False, text="", error=text)
        return AnalyzeResult(ok=False, text="", error="PDFからテキストを抽出できませんでした。")

    prompt = f"""
あなたは日本株の決算短信を読むプロのアナリストです。
以下はTDnetの決算短信PDFから抽出したテキストです。重要ポイントを短く、投資判断に使える形で整理してください。

【必須フォーマット】
- サマリ（3行以内）
- 業績ハイライト（売上/営業利益/経常/純利益。前年同期比・通期進捗・上方下方修正があれば明記）
- ガイダンス（通期予想、修正有無、前提）
- 注目ポイント（3〜6個：増減要因、セグメント、コスト、在庫、為替、特殊要因など）
- リスク/懸念（2〜5個）
- 次に見るべき資料（決算説明資料、IR、質疑、補足など）

【テキスト】
{text[:180000]}
""".strip()

    return gemini_generate(
        api_key=gemini_api_key,
        model=gemini_model,
        prompt=prompt,
        temperature=0.2,
    )
