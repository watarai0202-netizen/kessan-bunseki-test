import tempfile
from datetime import datetime, timezone

from src.storage import init_db, save_analysis, get_cached_analysis


def test_storage_roundtrip():
    with tempfile.NamedTemporaryFile(suffix=".db") as f:
        db_path = f.name
        init_db(db_path)

        url = "https://example.com/a.pdf"
        payload = {"summary_1min": "ok", "headline": {"tone": "中立", "score_0_10": 5}}
        save_analysis(
            db_path=db_path,
            doc_url=url,
            code="7203",
            title="決算短信",
            published_at=datetime.now(timezone.utc),
            payload=payload,
        )

        got = get_cached_analysis(db_path, url)
        assert got is not None
        assert got["summary_1min"] == "ok"
