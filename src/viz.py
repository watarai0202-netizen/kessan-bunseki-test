from __future__ import annotations

import streamlit as st


def render_analysis(payload: dict) -> None:
    # 1分要約
    st.markdown("#### 1分要約")
    st.write(payload.get("summary_1min", ""))

    # トーン/スコア
    headline = payload.get("headline") or {}
    tone = headline.get("tone", "不明")
    score = headline.get("score_0_10", None)
    st.markdown("#### トーン / スコア")
    st.write(f"トーン: {tone} / スコア: {score}")

    # YoY
    perf = payload.get("performance") or {}
    st.markdown("#### 前年比（%）")
    numeric = {}
    for k in ["sales_yoy_pct", "op_yoy_pct", "ordinary_yoy_pct", "net_yoy_pct"]:
        v = perf.get(k)
        if isinstance(v, (int, float)):
            numeric[k] = v

    if numeric:
        st.bar_chart(numeric)
    else:
        st.info("前年比の数値が取れませんでした（書式差の可能性）。")

    # ガイダンス
    guide = payload.get("guidance") or {}
    st.markdown("#### ガイダンス")
    st.write({
        "raised": guide.get("raised"),
        "lowered": guide.get("lowered"),
        "unchanged": guide.get("unchanged"),
        "sales_full_year": guide.get("sales_full_year"),
        "op_full_year": guide.get("op_full_year"),
        "eps_full_year": guide.get("eps_full_year"),
    })

    # 理由/リスク
    drivers = payload.get("drivers") or {}
    risks = payload.get("risks") or {}
    st.markdown("#### 増減益理由")
    st.write("増益理由:", drivers.get("profit_up_reasons", []))
    st.write("減益理由:", drivers.get("profit_down_reasons", []))

    st.markdown("#### リスク")
    st.write("短期:", risks.get("short_term", []))
    st.write("中期:", risks.get("mid_term", []))

    st.markdown("#### ウォッチポイント")
    st.write(payload.get("watch_points", []))
