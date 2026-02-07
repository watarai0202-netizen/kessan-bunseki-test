import hashlib

for i, it in enumerate(filtered[:100]):
    title = it.get("title", "")
    code_ = it.get("code", "")
    doc_url = (it.get("doc_url") or "").strip()
    link = (it.get("link") or "").strip()
    published = it.get("published_at")

    seed = f"{code_}|{published}|{title}|{doc_url}|{link}|{i}"
    uid = hashlib.md5(seed.encode("utf-8")).hexdigest()[:12]  # 12桁で十分

    with st.expander(f"{code_}｜{title}", expanded=False):
        cols = st.columns([1,1,2])

        with cols[0]:
            if st.button("キャッシュ表示", key=f"show_{uid}"):
                ...
        with cols[1]:
            run = st.button("AI分析", key=f"ai_{uid}", disabled=not can_run_ai)
import re

_KESSAN_RE = re.compile(r"(決算短信|四半期決算|通期決算|Financial Results|Earnings)", re.IGNORECASE)

def is_kessan(title: str) -> bool:
    return bool(_KESSAN_RE.search(title or ""))
