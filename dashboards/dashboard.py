"""
Dashboard — World Happiness Streaming Predictions
Workshop 3: ETL with Apache Kafka and Machine Learning
Streamlit + Plotly | Dark theme | Neon palette
"""

import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine
from pathlib import Path

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Happiness Predictions",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  /* Base */
  html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
      background-color: #0a0a0f !important;
  }
  [data-testid="stSidebar"] {
      background-color: #0f0f1a !important;
      border-right: 1px solid #1e1e3a;
  }
  [data-testid="stHeader"] { background: transparent !important; }

  /* Typography */
  h1, h2, h3, h4, h5 { color: #f0f0f0 !important; font-family: 'Inter', sans-serif; }
  p, li, label, span  { color: #b0b0c0 !important; }

  /* KPI cards */
  .kpi-wrapper {
      background: linear-gradient(135deg, #12122a 0%, #1a1a35 100%);
      border: 1px solid #2a2a50;
      border-radius: 16px;
      padding: 20px 16px;
      text-align: center;
      transition: transform 0.2s;
      box-shadow: 0 4px 20px rgba(0,0,0,0.4);
  }
  .kpi-wrapper:hover { transform: translateY(-2px); }
  .kpi-value {
      font-size: 2.1rem; font-weight: 800;
      font-family: 'Inter', monospace;
      letter-spacing: -1px;
  }
  .kpi-label {
      font-size: 0.78rem; color: #6666aa !important;
      text-transform: uppercase; letter-spacing: 1.5px;
      margin-top: 6px;
  }
  .kpi-delta { font-size: 0.72rem; margin-top: 4px; }

  /* Section header */
  .section-title {
      font-size: 1.05rem; font-weight: 700; color: #c0c0e0 !important;
      border-left: 3px solid #7209b7;
      padding-left: 10px; margin-bottom: 4px;
      text-transform: uppercase; letter-spacing: 1px;
  }

  /* Divider */
  .custom-divider {
      border: none; height: 1px;
      background: linear-gradient(90deg, transparent, #2a2a50, transparent);
      margin: 24px 0;
  }

  /* Streamlit widget overrides */
  [data-testid="stMultiSelect"] > div { background: #12122a; border-color: #2a2a50; }
  [data-testid="stDataFrame"] { background: #12122a; }
  .stDataFrame { border-radius: 12px; }

  /* Metric overrides */
  [data-testid="stMetric"] { background: #12122a; border-radius: 12px; padding: 12px; }
</style>
""", unsafe_allow_html=True)

# ── Color palette ─────────────────────────────────────────────────────────────
C = {
    "teal":   "#00f5d4",
    "pink":   "#f72585",
    "purple": "#7209b7",
    "blue":   "#3a86ff",
    "yellow": "#ffbe0b",
    "orange": "#fb5607",
    "violet": "#8338ec",
    "green":  "#06d6a0",
    "red":    "#ff4d6d",
}
PALETTE = list(C.values())

BG      = "#0a0a0f"
BG2     = "#12122a"
GRID    = "#1e1e3a"
BORDER  = "#2a2a50"
TEXT    = "#f0f0f0"
SUBTEXT = "#8888aa"


def dark_layout(**extra):
    base = dict(
        plot_bgcolor=BG2,
        paper_bgcolor=BG,
        font=dict(color=TEXT, family="Inter, sans-serif", size=12),
        margin=dict(l=16, r=16, t=36, b=16),
        legend=dict(
            bgcolor="rgba(18,18,42,0.8)",
            bordercolor=BORDER,
            borderwidth=1,
            font=dict(color=TEXT, size=11),
        ),
    )
    base.update(extra)
    return base


def style_axes(fig, xgrid=True, ygrid=True):
    fig.update_xaxes(
        gridcolor=GRID, linecolor=BORDER,
        showgrid=xgrid, zeroline=False,
        tickfont=dict(color=SUBTEXT, size=11),
        title_font=dict(color=SUBTEXT),
    )
    fig.update_yaxes(
        gridcolor=GRID, linecolor=BORDER,
        showgrid=ygrid, zeroline=False,
        tickfont=dict(color=SUBTEXT, size=11),
        title_font=dict(color=SUBTEXT),
    )
    return fig


# ── DB helpers ────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    host     = os.getenv("DB_HOST",     "localhost")
    port     = os.getenv("DB_PORT",     "5434")
    dbname   = os.getenv("DB_NAME",     "happiness_db")
    user     = os.getenv("DB_USER",     "etl_user")
    password = os.getenv("DB_PASSWORD", "etl_password")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")


@st.cache_data(ttl=10)
def load_predictions():
    sql = """
        SELECT fp.prediction_id,
               COALESCE(dc.country_name, 'Unknown') AS country,
               COALESCE(dd.year, 0)                 AS year,
               fp.actual_score,
               fp.predicted_score,
               fp.prediction_error,
               fp.gdp_per_capita,
               fp.social_support,
               fp.health_life_expectancy,
               fp.freedom,
               fp.generosity,
               fp.corruption,
               fp.prediction_timestamp
        FROM fact_predictions fp
        LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
        LEFT JOIN dim_date    dd ON fp.date_id    = dd.date_id
        ORDER BY fp.prediction_timestamp
    """
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=10)
def load_raw_status():
    with get_engine().connect() as conn:
        return pd.read_sql("""
            SELECT processing_status, COUNT(*) AS total
            FROM raw_happiness_events
            GROUP BY processing_status ORDER BY total DESC
        """, conn)


# ── KPI card HTML ─────────────────────────────────────────────────────────────
def kpi(label, value, color, subtitle=""):
    sub_html = f'<div class="kpi-delta" style="color:{SUBTEXT};">{subtitle}</div>' if subtitle else ""
    return f"""
    <div class="kpi-wrapper">
        <div class="kpi-value" style="color:{color};">{value}</div>
        <div class="kpi-label">{label}</div>
        {sub_html}
    </div>"""


def section(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)


def divider():
    st.markdown('<hr class="custom-divider">', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
try:
    df_all   = load_predictions()
    raw_stat = load_raw_status()
    ok = len(df_all) > 0
except Exception as e:
    st.error(f"**Database connection failed:** {e}")
    st.info("Make sure Docker is running: `docker-compose up -d`")
    st.stop()

if not ok:
    st.warning("No prediction data yet. Start the Kafka consumer and producer first.")
    st.stop()

df_all["year"] = df_all["year"].astype(int)
df_all["prediction_timestamp"] = pd.to_datetime(df_all["prediction_timestamp"])

# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"""
    <div style="text-align:center; padding: 16px 0 24px;">
        <div style="font-size:2.4rem;">🌍</div>
        <div style="font-size:1.1rem; font-weight:800; color:{C['teal']};">
            Happiness Pipeline
        </div>
        <div style="font-size:0.72rem; color:{SUBTEXT}; margin-top:4px;">
            Real-Time Predictions Dashboard
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f'<div style="color:{SUBTEXT}; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">Filters</div>', unsafe_allow_html=True)

    all_years     = sorted(df_all["year"].unique().tolist())
    sel_years     = st.multiselect("Year", all_years, default=all_years, key="yr")

    all_countries = sorted(df_all["country"].unique().tolist())
    sel_countries = st.multiselect("Country (all if empty)", all_countries, default=[], key="co")

    st.markdown("---")
    st.markdown(f'<div style="color:{SUBTEXT}; font-size:0.75rem; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px;">Pipeline Status</div>', unsafe_allow_html=True)

    total_raw  = int(raw_stat["total"].sum()) if len(raw_stat) > 0 else 0
    total_pred = len(df_all)
    valid_pct  = round(total_pred / total_raw * 100, 1) if total_raw > 0 else 0

    st.markdown(f"""
    <div style="background:{BG2}; border:1px solid {BORDER}; border-radius:10px; padding:14px;">
        <div style="margin-bottom:8px;">
            <span style="color:{SUBTEXT}; font-size:0.75rem;">Raw events</span><br>
            <span style="color:{C['teal']}; font-size:1.4rem; font-weight:700;">{total_raw}</span>
        </div>
        <div style="margin-bottom:8px;">
            <span style="color:{SUBTEXT}; font-size:0.75rem;">Valid predictions</span><br>
            <span style="color:{C['green']}; font-size:1.4rem; font-weight:700;">{total_pred}</span>
        </div>
        <div>
            <span style="color:{SUBTEXT}; font-size:0.75rem;">Success rate</span><br>
            <span style="color:{C['yellow']}; font-size:1.4rem; font-weight:700;">{valid_pct}%</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    auto_refresh = st.toggle("Auto-refresh (10 s)", value=False)
    if auto_refresh:
        import time; time.sleep(10); st.rerun()

# ── Apply filters ─────────────────────────────────────────────────────────────
df = df_all.copy()
if sel_years:
    df = df[df["year"].isin(sel_years)]
if sel_countries:
    df = df[df["country"].isin(sel_countries)]

if len(df) == 0:
    st.warning("No data for the selected filters.")
    st.stop()

# ── Compute summary metrics ───────────────────────────────────────────────────
avg_err   = df["prediction_error"].mean()
avg_act   = df["actual_score"].mean()
avg_pred  = df["predicted_score"].mean()
ss_res    = ((df["actual_score"] - df["predicted_score"]) ** 2).sum()
ss_tot    = ((df["actual_score"] - df["actual_score"].mean()) ** 2).sum()
r2_live   = 1 - ss_res / ss_tot if ss_tot > 0 else 0
n_ctry    = df["country"].nunique()
best_ctry = df.groupby("country")["predicted_score"].mean().idxmax()

# ══════════════════════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown(f"""
<div style="padding: 8px 0 24px;">
    <div style="font-size:2rem; font-weight:900; letter-spacing:-1px;
                background:linear-gradient(90deg,{C['teal']},{C['blue']},{C['pink']});
                -webkit-background-clip:text; -webkit-text-fill-color:transparent;">
        World Happiness — Real-Time ML Predictions
    </div>
    <div style="font-size:0.85rem; color:{SUBTEXT}; margin-top:4px;">
        ETL Workshop 3 &nbsp;·&nbsp; Apache Kafka + PostgreSQL + Scikit-learn &nbsp;·&nbsp;
        Showing <b style="color:{C['teal']};">{len(df)}</b> predictions across
        <b style="color:{C['teal']};">{n_ctry}</b> countries
    </div>
</div>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  ROW 1 — KPI CARDS
# ══════════════════════════════════════════════════════════════════════════════
cols = st.columns(6)
cards = [
    ("Avg Error",       f"{avg_err:.4f}",         C["pink"],   "MAE over filtered set"),
    ("Avg Actual",      f"{avg_act:.3f}",          C["teal"],   "Mean happiness score"),
    ("Avg Predicted",   f"{avg_pred:.3f}",         C["blue"],   "Model output average"),
    ("R² (live)",       f"{r2_live:.4f}",          C["yellow"], "Coefficient of determination"),
    ("Top Country",     best_ctry[:12],            C["green"],  "Highest avg predicted score"),
    ("Success Rate",    f"{valid_pct:.1f}%",       C["violet"], f"{total_pred} of {total_raw} events"),
]
for col, (lbl, val, color, sub) in zip(cols, cards):
    col.markdown(kpi(lbl, val, color, sub), unsafe_allow_html=True)

divider()

# ══════════════════════════════════════════════════════════════════════════════
#  ROW 2 — PREDICTED vs ACTUAL  +  ERROR HISTOGRAM
# ══════════════════════════════════════════════════════════════════════════════
section("KPI 3 — Predicted vs Actual Score & Error Distribution")
st.markdown("")

col_scatter, col_hist = st.columns([3, 2], gap="medium")

with col_scatter:
    year_colors = {y: PALETTE[i % len(PALETTE)] for i, y in enumerate(sorted(df["year"].unique()))}
    fig = go.Figure()

    for yr, grp in df.groupby("year"):
        fig.add_trace(go.Scatter(
            x=grp["actual_score"],
            y=grp["predicted_score"],
            mode="markers",
            name=str(yr),
            marker=dict(
                color=year_colors[yr], size=9, opacity=0.75,
                line=dict(color="rgba(0,0,0,0.4)", width=0.8),
                symbol="circle",
            ),
            hovertemplate=(
                "<b>%{customdata[0]}</b> (%{customdata[1]})<br>"
                "Actual: %{x:.3f}<br>Predicted: %{y:.3f}<br>"
                "Error: %{customdata[2]:.4f}<extra></extra>"
            ),
            customdata=list(zip(grp["country"], grp["year"], grp["prediction_error"])),
        ))

    lo = min(df["actual_score"].min(), df["predicted_score"].min()) - 0.2
    hi = max(df["actual_score"].max(), df["predicted_score"].max()) + 0.2
    fig.add_trace(go.Scatter(
        x=[lo, hi], y=[lo, hi], mode="lines",
        line=dict(color="rgba(255,255,255,0.25)", width=1.5, dash="dot"),
        name="Perfect fit", hoverinfo="skip",
    ))

    fig.update_layout(
        **dark_layout(
            height=380,
            xaxis_title="Actual Happiness Score",
            yaxis_title="Predicted Happiness Score",
            title=dict(text="Predicted vs Actual", font=dict(size=13, color=SUBTEXT), x=0),
        )
    )
    style_axes(fig)
    st.plotly_chart(fig, width="stretch")

with col_hist:
    fig2 = go.Figure()
    fig2.add_trace(go.Histogram(
        x=df["prediction_error"],
        nbinsx=28,
        marker=dict(
            color=C["pink"],
            opacity=0.85,
            line=dict(color=BG, width=0.6),
        ),
        hovertemplate="Error bucket: %{x:.3f}<br>Count: %{y}<extra></extra>",
    ))
    fig2.add_vline(
        x=avg_err, line_color=C["yellow"], line_dash="dash", line_width=2,
        annotation_text=f"  avg {avg_err:.3f}",
        annotation_font=dict(color=C["yellow"], size=11),
        annotation_position="top right",
    )
    fig2.update_layout(
        **dark_layout(
            height=380,
            xaxis_title="|Actual − Predicted|",
            yaxis_title="Count",
            title=dict(text="Error Distribution", font=dict(size=13, color=SUBTEXT), x=0),
            showlegend=False,
        )
    )
    style_axes(fig2)
    st.plotly_chart(fig2, width="stretch")

divider()

# ══════════════════════════════════════════════════════════════════════════════
#  ROW 3 — TOP COUNTRIES BAR  +  DONUT STATUS
# ══════════════════════════════════════════════════════════════════════════════
section("KPI 2 — Rankings & Pipeline Health")
st.markdown("")

col_bar, col_donut = st.columns([3, 2], gap="medium")

with col_bar:
    top_n = 20
    ctry_agg = (
        df.groupby("country")
        .agg(avg_pred=("predicted_score", "mean"),
             avg_actual=("actual_score", "mean"),
             avg_err=("prediction_error", "mean"),
             count=("prediction_id", "count"))
        .reset_index()
        .sort_values("avg_pred", ascending=True)
        .tail(top_n)
    )

    # Color gradient by score
    score_norm = (ctry_agg["avg_pred"] - ctry_agg["avg_pred"].min()) / \
                 (ctry_agg["avg_pred"].max() - ctry_agg["avg_pred"].min() + 1e-9)
    bar_colors = [
        f"rgba({int(58 + (0 - 58) * v)},{int(134 + (245 - 134) * v)},{int(255 + (212 - 255) * v)},0.85)"
        for v in score_norm
    ]

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        y=ctry_agg["country"],
        x=ctry_agg["avg_pred"],
        orientation="h",
        marker=dict(color=bar_colors, line=dict(color=BG, width=0.5)),
        text=[f"{v:.3f}" for v in ctry_agg["avg_pred"]],
        textposition="outside",
        textfont=dict(color=TEXT, size=10),
        customdata=list(zip(ctry_agg["avg_actual"], ctry_agg["avg_err"], ctry_agg["count"])),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Predicted: %{x:.3f}<br>"
            "Actual: %{customdata[0]:.3f}<br>"
            "Avg Error: %{customdata[1]:.4f}<br>"
            "Records: %{customdata[2]}<extra></extra>"
        ),
    ))
    fig3.update_layout(
        **dark_layout(
            height=520,
            xaxis_title="Avg Predicted Happiness Score",
            title=dict(text=f"Top {top_n} Countries by Predicted Score", font=dict(size=13, color=SUBTEXT), x=0),
            showlegend=False,
        )
    )
    style_axes(fig3, ygrid=False)
    fig3.update_yaxes(tickfont=dict(size=10))
    st.plotly_chart(fig3, width="stretch")

with col_donut:
    if len(raw_stat) > 0:
        status_colors = {
            "VALID":            C["green"],
            "INVALID_SCHEMA":   C["red"],
            "INVALID_VALUES":   C["orange"],
            "PREDICTION_ERROR": C["yellow"],
        }
        donut_colors = [status_colors.get(s, C["blue"]) for s in raw_stat["processing_status"]]

        fig4 = go.Figure(go.Pie(
            labels=raw_stat["processing_status"],
            values=raw_stat["total"],
            hole=0.62,
            marker=dict(colors=donut_colors, line=dict(color=BG, width=3)),
            textinfo="none",
            hovertemplate="<b>%{label}</b><br>Count: %{value}<br>%{percent}<extra></extra>",
        ))
        fig4.update_layout(
            **dark_layout(
                height=240,
                title=dict(text="Event Processing Status", font=dict(size=13, color=SUBTEXT), x=0),
                margin=dict(l=16, r=16, t=36, b=0),
                annotations=[dict(
                    text=f"<b>{total_raw}</b><br><span style='font-size:10px'>events</span>",
                    x=0.5, y=0.5, xanchor="center", yanchor="middle",
                    font=dict(size=16, color=TEXT), showarrow=False,
                )],
            )
        )
        st.plotly_chart(fig4, width="stretch")

        # Status breakdown legend
        for _, row in raw_stat.iterrows():
            status = row["processing_status"]
            color  = status_colors.get(status, C["blue"])
            pct    = row["total"] / total_raw * 100
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;'
                f'background:{BG2};border:1px solid {BORDER};border-radius:8px;'
                f'padding:8px 14px;margin-bottom:6px;">'
                f'<span style="color:{color};font-weight:700;font-size:0.8rem;">● {status}</span>'
                f'<span style="color:{TEXT};font-size:0.8rem;">{int(row["total"])} &nbsp;'
                f'<span style="color:{SUBTEXT};">({pct:.1f}%)</span></span>'
                f'</div>',
                unsafe_allow_html=True,
            )

divider()

# ══════════════════════════════════════════════════════════════════════════════
#  ROW 4 — TRENDS OVER TIME
# ══════════════════════════════════════════════════════════════════════════════
section("KPI 4 — Prediction Trends Over Time")
st.markdown("")

df_ts = df.sort_values("prediction_timestamp").copy()
df_ts["event_num"] = range(1, len(df_ts) + 1)

fig5 = go.Figure()
fig5.add_trace(go.Scatter(
    x=df_ts["event_num"], y=df_ts["actual_score"],
    mode="lines", name="Actual Score",
    line=dict(color=C["teal"], width=2),
    hovertemplate="Event #%{x}<br>Actual: %{y:.3f}<extra></extra>",
))
fig5.add_trace(go.Scatter(
    x=df_ts["event_num"], y=df_ts["predicted_score"],
    mode="lines", name="Predicted Score",
    line=dict(color=C["pink"], width=2, dash="dash"),
    hovertemplate="Event #%{x}<br>Predicted: %{y:.3f}<extra></extra>",
))
# Rolling average of actual
if len(df_ts) >= 10:
    roll = df_ts["actual_score"].rolling(10, center=True).mean()
    fig5.add_trace(go.Scatter(
        x=df_ts["event_num"], y=roll,
        mode="lines", name="10-event avg (actual)",
        line=dict(color=C["yellow"], width=1.5, dash="dot"),
        hovertemplate="Event #%{x}<br>Rolling avg: %{y:.3f}<extra></extra>",
    ))

fig5.update_layout(
    **dark_layout(
        height=320,
        xaxis_title="Event Number (streaming order)",
        yaxis_title="Happiness Score",
        title=dict(text="Actual vs Predicted — Streaming Timeline", font=dict(size=13, color=SUBTEXT), x=0),
    )
)
style_axes(fig5)
st.plotly_chart(fig5, width="stretch")

divider()

# ══════════════════════════════════════════════════════════════════════════════
#  ROW 5 — YEAR COMPARISON + FEATURES
# ══════════════════════════════════════════════════════════════════════════════
section("KPI 1 — Error Analysis & Feature Averages by Year")
st.markdown("")

col_err, col_feat = st.columns(2, gap="medium")

with col_err:
    year_stats = (
        df.groupby("year")
        .agg(avg_err=("prediction_error", "mean"),
             min_err=("prediction_error", "min"),
             max_err=("prediction_error", "max"),
             count=("prediction_id", "count"))
        .reset_index()
    )
    fig6 = go.Figure()
    fig6.add_trace(go.Bar(
        x=year_stats["year"].astype(str),
        y=year_stats["avg_err"],
        name="Avg Error",
        marker=dict(
            color=[PALETTE[i % len(PALETTE)] for i in range(len(year_stats))],
            opacity=0.85,
            line=dict(color=BG, width=1),
        ),
        text=[f"{v:.4f}" for v in year_stats["avg_err"]],
        textposition="outside",
        textfont=dict(color=TEXT, size=11),
        hovertemplate="Year: %{x}<br>Avg Error: %{y:.4f}<br>Records: %{customdata}<extra></extra>",
        customdata=year_stats["count"],
    ))
    fig6.add_trace(go.Scatter(
        x=year_stats["year"].astype(str),
        y=year_stats["max_err"],
        mode="markers", name="Max Error",
        marker=dict(color=C["red"], size=10, symbol="diamond"),
        hovertemplate="Max Error %{x}: %{y:.4f}<extra></extra>",
    ))
    fig6.update_layout(
        **dark_layout(
            height=320,
            xaxis_title="Year",
            yaxis_title="Prediction Error",
            title=dict(text="Avg Prediction Error by Year", font=dict(size=13, color=SUBTEXT), x=0),
        )
    )
    style_axes(fig6)
    st.plotly_chart(fig6, width="stretch")

with col_feat:
    feat_cols = ["gdp_per_capita", "social_support", "health_life_expectancy",
                 "freedom", "generosity", "corruption"]
    feat_labels = ["GDP", "Social Support", "Health", "Freedom", "Generosity", "Corruption"]

    feat_year = df.groupby("year")[feat_cols].mean().reset_index()

    fig7 = go.Figure()
    for i, row in feat_year.iterrows():
        yr = int(row["year"])
        fig7.add_trace(go.Scatterpolar(
            r=[row[c] for c in feat_cols] + [row[feat_cols[0]]],
            theta=feat_labels + [feat_labels[0]],
            name=str(yr),
            mode="lines+markers",
            line=dict(color=PALETTE[i % len(PALETTE)], width=2),
            marker=dict(size=6),
            hovertemplate="%{theta}: %{r:.3f}<extra>" + str(yr) + "</extra>",
        ))
    fig7.update_layout(
        **dark_layout(
            height=320,
            title=dict(text="Feature Radar by Year", font=dict(size=13, color=SUBTEXT), x=0),
            polar=dict(
                bgcolor=BG2,
                angularaxis=dict(
                    gridcolor=GRID, linecolor=BORDER,
                    tickfont=dict(color=TEXT, size=10),
                ),
                radialaxis=dict(
                    gridcolor=GRID, linecolor=BORDER,
                    tickfont=dict(color=SUBTEXT, size=9),
                    showticklabels=True,
                ),
            ),
        )
    )
    st.plotly_chart(fig7, width="stretch")

divider()

# ══════════════════════════════════════════════════════════════════════════════
#  ROW 6 — SCATTER MATRIX + DATA TABLE
# ══════════════════════════════════════════════════════════════════════════════
section("Deep Dive — Feature Correlation & Recent Events")
st.markdown("")

col_corr, col_table = st.columns([2, 3], gap="medium")

with col_corr:
    num_cols = ["happiness_score" if "happiness_score" in df.columns else "actual_score",
                "gdp_per_capita", "social_support", "health_life_expectancy",
                "freedom", "generosity", "corruption"]
    available = [c for c in num_cols if c in df.columns]
    corr_df = df[available].copy()
    if "happiness_score" not in corr_df.columns:
        corr_df = corr_df.rename(columns={"actual_score": "happiness_score"})
    corr_matrix = corr_df.rename(columns={
        "happiness_score": "Happiness", "gdp_per_capita": "GDP",
        "social_support": "Social", "health_life_expectancy": "Health",
        "freedom": "Freedom", "generosity": "Generosity", "corruption": "Corruption",
    }).corr()

    fig8 = go.Figure(go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns.tolist(),
        y=corr_matrix.index.tolist(),
        colorscale=[
            [0.0,  "#7209b7"],
            [0.25, "#3a0ca3"],
            [0.5,  "#12122a"],
            [0.75, "#0077b6"],
            [1.0,  "#00f5d4"],
        ],
        zmid=0,
        text=np.round(corr_matrix.values, 2),
        texttemplate="%{text}",
        textfont=dict(size=10, color="white"),
        hovertemplate="%{y} vs %{x}<br>r = %{z:.3f}<extra></extra>",
        showscale=True,
        colorbar=dict(
            thickness=12, len=0.8,
            tickfont=dict(color=SUBTEXT, size=9),
            outlinecolor=BORDER,
        ),
    ))
    fig8.update_layout(
        **dark_layout(
            height=320,
            title=dict(text="Correlation Heatmap", font=dict(size=13, color=SUBTEXT), x=0),
            margin=dict(l=16, r=16, t=36, b=16),
        )
    )
    style_axes(fig8, xgrid=False, ygrid=False)
    fig8.update_xaxes(tickangle=-30, tickfont=dict(size=10))
    fig8.update_yaxes(tickfont=dict(size=10))
    st.plotly_chart(fig8, width="stretch")

with col_table:
    show = (
        df[["country", "year", "actual_score", "predicted_score",
            "prediction_error", "prediction_timestamp"]]
        .sort_values("prediction_timestamp", ascending=False)
        .head(60)
        .copy()
    )
    show.columns = ["Country", "Year", "Actual", "Predicted", "Error", "Timestamp"]
    show["Timestamp"] = show["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Color error column
    def color_error(val):
        if val < 0.2:  return "color: #06d6a0; font-weight:600;"
        if val < 0.5:  return "color: #ffbe0b; font-weight:600;"
        return "color: #f72585; font-weight:600;"

    styled = (
        show.style
        .format({"Actual": "{:.3f}", "Predicted": "{:.3f}", "Error": "{:.4f}"})
        .map(color_error, subset=["Error"])
        .set_properties(**{
            "background-color": BG2,
            "color": TEXT,
            "border-color": BORDER,
            "font-size": "12px",
        })
        .set_table_styles([{
            "selector": "th",
            "props": [
                ("background-color", "#1a1a35"),
                ("color", SUBTEXT),
                ("font-size", "11px"),
                ("text-transform", "uppercase"),
                ("letter-spacing", "1px"),
                ("border-bottom", f"1px solid {BORDER}"),
            ]
        }])
    )
    st.dataframe(styled, width="stretch", height=330)

# ══════════════════════════════════════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════════════════════════════════════
divider()
st.markdown(f"""
<div style="text-align:center; padding:12px 0 4px;">
    <span style="color:{SUBTEXT}; font-size:0.75rem; letter-spacing:1px;">
        ETL WORKSHOP 3 &nbsp;·&nbsp; APACHE KAFKA &nbsp;·&nbsp; POSTGRESQL &nbsp;·&nbsp;
        SCIKIT-LEARN &nbsp;·&nbsp; STREAMLIT + PLOTLY &nbsp;·&nbsp;
        Universidad Autónoma de Occidente
    </span>
</div>
""", unsafe_allow_html=True)
