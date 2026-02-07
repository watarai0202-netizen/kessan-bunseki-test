from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional


def db_path_default() -> str:
    # Streamlit Cloudでも書き込み可能な場所をデフォルトに
    return "/tmp/app.db"


def _connect(db_path: str) -> sqlite3.Connection:
    # 同時アクセス耐性ちょい改善
    con = sqlite3.connect(db_path, timeout=30)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db(db_path: str) -> None:
    if "/" in db_path:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

    con = _connect(db_path)
    cur = con.cursor()

    # 既存互換のまま作成
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS analyses (
          doc_url TEXT PRIMARY KEY,
          code TEXT,
          title TEXT,
          published_at TEXT,
          payload_json TEXT,
          created_at TEXT
        )
        """
    )

    # ---- 追加カラム（存在しなければ追加）----
    _ensure_column(cur, "analyses", "model", "TEXT")
    _ensure_column(cur, "analyses", "tokens", "INTEGER")
    _ensure_column(cur, "analyses", "schema_version", "INTEGER")

    con.commit()
    con.close()


def _ensure_column(cur: sqlite3.Cursor, table: str, col: str, coltype: str) -> None:
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = name
    if col in cols:
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")


def get_cached_analysis(db_path: str, doc_url: str) -> dict | None:
    if not doc_url:
        return None

    con = _connect(db_path)
    cur = con.cursor()

    # 新カラムが無い古いDBでも動くように、まずpayload_jsonだけ取る
    try:
        cur.execute(
            "SELECT payload_json, model, tokens, schema_version FROM analyses WHERE doc_url=?",
            (doc_url,),
        )
        row = cur.fetchone()
        con.close()
        if not row:
            return None

        payload_raw = row[0]
        model = row[1]
        tokens = row[2]
        schema_version = row[3]
    except Exception:
        # 古いスキーマ
        cur.execute("SELECT payload_json FROM analyses WHERE doc_url=?", (doc_url,))
        row = cur.fetchone()
        con.close()
        if not row:
            return None
        payload_raw = row[0]
        model = None
        tokens = None
        schema_version = None

    try:
        payload = json.loads(payload_raw)
        if isinstance(payload, dict):
            # DBメタがあれば補完（payload側に無ければ）
            if model and "model" not in payload:
                payload["model"] = model
            if tokens is not None and "tokens" not in payload:
                payload["tokens"] = tokens
            if schema_version is not None and "schema_version" not in payload:
                payload["schema_version"] = schema_version
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def save_analysis(
    db_path: str,
    doc_url: str,
    code: str,
    title: str,
    published_at,
    payload: dict,
) -> None:
    """
    payload_json はそのまま保存（後方互換維持）。
    追加で model/tokens/schema_version を「取れる範囲で」保存。
    """
    if not doc_url:
        return

    published_str = ""
    if published_at is not None:
        try:
            published_str = published_at.astimezone(timezone.utc).isoformat()
        except Exception:
            published_str = str(published_at)

    model = _infer_model(payload)
    tokens = _infer_tokens(payload)
    schema_version = _infer_schema_version(payload)

    con = _connect(db_path)
    cur = con.cursor()

    # 追加カラムがない古いDBでも落ちないように、tryで分岐
    try:
        cur.execute(
            """
            INSERT OR REPLACE INTO analyses
              (doc_url, code, title, published_at, payload_json, created_at, model, tokens, schema_version)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                doc_url,
                code,
                title,
                published_str,
                json.dumps(payload, ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
                model,
                tokens,
                schema_version,
            ),
        )
    except Exception:
        # 古いDB（追加カラムなし）向け
        cur.execute(
            """
            INSERT OR REPLACE INTO analyses
              (doc_url, code, title, published_at, payload_json, created_at)
            VALUES (?,?,?,?,?,?)
            """,
            (
                doc_url,
                code,
                title,
                published_str,
                json.dumps(payload, ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    con.commit()
    con.close()


# ----------------------------
# inference helpers
# ----------------------------

def _infer_model(payload: dict) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    v = payload.get("model")
    return str(v).strip() if v else None


def _infer_tokens(payload: dict) -> Optional[int]:
    if not isinstance(payload, dict):
        return None
    v = payload.get("tokens")
    if isinstance(v, int):
        return v
    # usage_metadataをresultに入れる設計にしたくなった時の保険
    try:
        if isinstance(v, str) and v.isdigit():
            return int(v)
    except Exception:
        pass
    return None


def _infer_schema_version(payload: dict) -> Optional[int]:
    """
    あなたの新スキーマは:
      payload = {ok, pdf_url, model, tokens, result:{...}}
    という構造なので、とりあえず version=2 扱いにする。
    旧payloadは version=1 の想定。
    """
    if not isinstance(payload, dict):
        return None
    if "schema_version" in payload and isinstance(payload["schema_version"], int):
        return payload["schema_version"]

    # 新スキーマ判定
    if isinstance(payload.get("result"), dict):
        return 2

    # 旧スキーマっぽいキー
    if any(k in payload for k in ("summary_1min", "headline", "watch_points")):
        return 1

    return None
