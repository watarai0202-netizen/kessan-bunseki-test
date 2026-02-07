from src.tdnet import _normalize_item


def test_normalize_item_basic():
    raw = {
        "TDnet": {
            "title": "2026年3月期 第3四半期決算短信",
            "code": "7203",
            "document_url": "https://example.com/a.pdf",
            "published_at": "2026-02-06T00:00:00Z",
        }
    }
    it = _normalize_item(raw)
    assert it["title"]
    assert it["code"] == "7203"
    assert it["doc_url"].endswith(".pdf")
    assert it["published_at"] is not None


def test_normalize_item_variants():
    raw = {
        "title": "決算短信",
        "Code": 1234,
        "url": "https://example.com/b.pdf",
        "date": "2026-02-06T09:00:00+09:00",
    }
    it = _normalize_item(raw)
    assert it["code"] == "1234"
    assert it["doc_url"]
