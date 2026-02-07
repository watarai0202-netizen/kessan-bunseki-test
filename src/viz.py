from __future__ import annotations

from typing import Any, Dict, Optional

import streamlit as st


# ----------------------------
# formatting helpers
# ----------------------------

def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _fmt_num(x: Any) -> str:
    """
    LLM抽出の単位が揺れるので、ここでは “見やすい” に寄せるだけ。
    - int/float: 3桁区切り
    - それ以外: 文字列化
    """
    if x is None:
        return "—"
    if _is_number(x):
        # なるべく整数っぽく見せる
        if isinstance(x, float) and abs(x - int(x)) < 1e-9:
            x = int(x)
        return f"{x:,}"
    s = str(x).strip()
    return s if s else "—"


def _fmt_pct(x: Any) -> str:
    if x is None:
        return "—"
    if _is_number(x):
        return f"{x:.1f}%"
    s = str(x).strip()
    return s if s else "—"


def _fmt_delta_pct(x: Any) -> Optional[str]:
    """
    st.metric の delta は None だと非表示にできるので、
    数字が取れたときだけ delta 文字列を返す。
    """
    if _is_number(x):
        sign = "+" if x > 0 else ""
        return f"{sign}{x:.1f}%"
    return None


def _as_list(x: Any) -> list[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    if isinstance(x, str) and x.strip():
        return [x.strip()]
    return []


def _progress_value(v: Any) -> Optional[float]:
    """
    進捗の値が
      - 0〜1 の比率
      - 0〜100 の%
    のどっちで来ても対応する。
    """
    if not _is_number(v):
        return None
    x = float(v)
    if x < 0:
        return 0.0
    if x <= 1.0:
        return x
    # 2〜100 ぐらいなら%扱い
    if x <= 100.0:
        return x / 100.0
    # 異常にでかい数は不明として捨てる
    return None


# ----------------------------
# schema normalization
# ----------------------------

def _pick_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    新スキーマ: payload["result"] が本体
    旧スキーマ: payload 自体が本体
    """
    if isinstance(payload.get("result"), dict):
        return payload["result"]  # type: ignore[return-value]
    return payload


def _meta_line(payload: Dict[str, Any]) -> str:
    model = payload.get("model")
    tokens = payload.get("tokens")
    parts = []
    if model:
        parts.append(f"model: {model}")
    if tokens is not None:
        parts.append(f"tokens: {tokens}")
    return " / ".join(parts)


# ----------------------------
# Public
# ----------------------------

def render_analysis(payload: dict) -> None:
    """
    app.py から呼ばれる表示関数。
    “一目で分かる” を優先して、metric + 進捗 + 箇条書きに寄せる。
    """
    if not isinstance(payload, dict):
        st.error("解析データが不正です（dictではありません）。")
        return

    # OK/エラー
    if payload.get("ok") is False:
        st.error(payload.get("error") or "解析に失敗しました。")
        meta = _meta_line(payload)
        if meta:
            st.caption(meta)
        return

    result = _pick_result(payload)
    meta = _meta_line(payload)
    if meta:
        st.caption(meta)

    # ----------------------------
    # Summary（3行以内）
    # ----------------------------
    summary = (
        result.get("summary")
        or result.get("summary_1min")
        or ""
    )
    st.markdown("### 🧾 1分カード")
    if isinstance(summary, str) and summary.strip():
        st.write(summary.strip())
    else:
        st.info("サマリが取得できませんでした。")

    # ----------------------------
    # 主要数値：売上 / 営業 / 経常 / 純利
    # ----------------------------
    perf = result.get("performance") or {}
    # 旧スキーマ互換（もし存在するなら）
    if not isinstance(perf, dict):
        perf = {}

    yoy = perf.get("yoy") or {}
    if not isinstance(yoy, dict):
        yoy = {}

    # 旧スキーマの yoy %（sales_yoy_pct 等）にも救済対応
    legacy_yoy_map = {
        "sales": result.get("performance", {}).get("sales_yoy_pct") if isinstance(result.get("performance"), dict) else None,
        "op_profit": result.get("performance", {}).get("op_yoy_pct") if isinstance(result.get("performance"), dict) else None,
        "ordinary_profit": result.get("performance", {}).get("ordinary_yoy_pct") if isinstance(result.get("performance"), dict) else None,
        "net_profit": result.get("performance", {}).get("net_yoy_pct") if isinstance(result.get("performance"), dict) else None,
    }

    # 値（数値 or null）
    sales = perf.get("sales")
    op = perf.get("op_profit")
    ordinary = perf.get("ordinary_profit")
    net = perf.get("net_profit")

    # YoY（%）は新スキーマ yoy.{...} を優先し、無ければ旧を拾う
    sales_yoy = yoy.get("sales", legacy_yoy_map["sales"])
    op_yoy = yoy.get("op_profit", legacy_yoy_map["op_profit"])
    ord_yoy = yoy.get("ordinary_profit", legacy_yoy_map["ordinary_profit"])
    net_yoy = yoy.get("net_profit", legacy_yoy_map["net_profit"])

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("売上高", _fmt_num(sales), delta=_fmt_delta_pct(sales_yoy))
    with c2:
        st.metric("営業利益", _fmt_num(op), delta=_fmt_delta_pct(op_yoy))
    with c3:
        st.metric("経常利益", _fmt_num(ordinary), delta=_fmt_delta_pct(ord_yoy))
    with c4:
        st.metric("純利益", _fmt_num(net), delta=_fmt_delta_pct(net_yoy))

    # ----------------------------
    # 進捗（通期）
    # ----------------------------
    prog = perf.get("progress_full_year") or {}
    if not isinstance(prog, dict):
        prog = {}

    prog_sales = _progress_value(prog.get("sales"))
    prog_op = _progress_value(prog.get("op_profit"))
    prog_ord = _progress_value(prog.get("ordinary_profit"))
    prog_net = _progress_value(prog.get("net_profit"))

    if any(v is not None for v in [prog_sales, prog_op, prog_ord, prog_net]):
        st.markdown("#### 📊 通期進捗")
        pc1, pc2 = st.columns(2)
        with pc1:
            if prog_sales is not None:
                st.write(f"売上高：{prog_sales*100:.1f}%")
                st.progress(prog_sales)
            if prog_op is not None:
                st.write(f"営業利益：{prog_op*100:.1f}%")
                st.progress(prog_op)
        with pc2:
            if prog_ord is not None:
                st.write(f"経常利益：{prog_ord*100:.1f}%")
                st.progress(prog_ord)
            if prog_net is not None:
                st.write(f"純利益：{prog_net*100:.1f}%")
                st.progress(prog_net)

    # ----------------------------
    # 修正（上方/下方/据置など）
    # ----------------------------
    rev = perf.get("revision") or {}
    if not isinstance(rev, dict):
        rev = {}

    rev_exists = rev.get("exists")
    rev_dir = rev.get("direction")
    rev_reason = rev.get("reason")

    if rev_exists is not None or rev_dir or rev_reason:
        st.markdown("#### 🧭 修正")
        msg = []
        if rev_exists is True:
            msg.append("修正あり")
        elif rev_exists is False:
            msg.append("修正なし")
        if rev_dir:
            msg.append(f"方向: {rev_dir}")
        if rev_reason:
            msg.append(f"理由: {rev_reason}")
        st.write(" / ".join([str(m) for m in msg if str(m).strip()]))

    # ----------------------------
    # ガイダンス（通期予想）
    # ----------------------------
    guide = result.get("guidance") or {}
    if not isinstance(guide, dict):
        guide = {}

    fy = guide.get("full_year_forecast") or {}
    if not isinstance(fy, dict):
        fy = {}

    has_any_forecast = any(fy.get(k) is not None for k in ("sales", "op_profit", "ordinary_profit", "net_profit"))
    assumptions = _as_list(guide.get("assumptions"))
    notes = guide.get("notes")

    if has_any_forecast or assumptions or (isinstance(notes, str) and notes.strip()):
        st.markdown("#### 🗓️ ガイダンス（通期予想）")
        g1, g2, g3, g4 = st.columns(4)
        with g1:
            st.metric("予想 売上高", _fmt_num(fy.get("sales")))
        with g2:
            st.metric("予想 営業利益", _fmt_num(fy.get("op_profit")))
        with g3:
            st.metric("予想 経常利益", _fmt_num(fy.get("ordinary_profit")))
        with g4:
            st.metric("予想 純利益", _fmt_num(fy.get("net_profit")))

        if assumptions:
            with st.expander("前提（assumptions）", expanded=False):
                st.write(assumptions)
        if isinstance(notes, str) and notes.strip():
            st.caption(notes.strip())

    # ----------------------------
    # 箇条書き（見どころ/リスク/次に見るもの）
    # ----------------------------
    highlights = _as_list(result.get("highlights") or result.get("watch_points"))
    risks = _as_list(result.get("risks"))
    next_to_check = _as_list(result.get("next_to_check"))

    # 旧スキーマ risks: {short_term, mid_term} っぽい場合救済
    if not risks and isinstance(result.get("risks"), dict):
        rdict = result.get("risks")  # type: ignore[assignment]
        risks = _as_list(rdict.get("short_term")) + _as_list(rdict.get("mid_term"))

    cols = st.columns(3)
    with cols[0]:
        st.markdown("#### ✅ 見どころ")
        if highlights:
            st.write(highlights)
        else:
            st.write(["—"])
    with cols[1]:
        st.markdown("#### ⚠️ リスク")
        if risks:
            st.write(risks)
        else:
            st.write(["—"])
    with cols[2]:
        st.markdown("#### 🔎 次に見るもの")
        if next_to_check:
            st.write(next_to_check)
        else:
            st.write(["—"])
