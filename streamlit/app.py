"""
streamlit/app.py — FRED Economic Indicators Dashboard

Displays economic trends, recession risk signals, and
indicator deep-dives from the FRED Financial Pipeline.

Run with: streamlit run streamlit/app.py
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ============================================
# Page Config
# ============================================
st.set_page_config(
    page_title="FRED Economic Dashboard",
    page_icon="📊",
    layout="wide",
)

# ============================================
# Snowflake Connection
# ============================================
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database="FRED_PIPELINE",
        warehouse="FRED_WH",
        role="ACCOUNTADMIN",
    )

@st.cache_data(ttl=3600)  # refresh every hour
def load_dashboard_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM MARTS.MART_ECONOMIC_DASHBOARD ORDER BY observation_month", conn)
    df.columns = [c.lower() for c in df.columns]  # Snowflake returns UPPERCASE
    df["observation_month"] = pd.to_datetime(df["observation_month"])
    return df

@st.cache_data(ttl=3600)
def load_summary_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM MARTS.MART_INDICATOR_SUMMARY", conn)
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=3600)
def load_ingestion_log():
    conn = get_connection()
    df = pd.read_sql(
        """SELECT run_id, series_id, records_fetched, status, 
                  started_at, completed_at, error_message
           FROM RAW.INGESTION_LOG 
           ORDER BY started_at DESC 
           LIMIT 20""",
        conn,
    )
    df.columns = [c.lower() for c in df.columns]
    return df


# ============================================
# Load data
# ============================================
df = load_dashboard_data()
summary = load_summary_data()

# ============================================
# Sidebar
# ============================================
st.sidebar.title("📊 FRED Pipeline Dashboard")
st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigate",
    ["Economic Overview", "Recession Risk Monitor", "Indicator Deep Dive", "Data Quality"],
)

# Date range filter
st.sidebar.markdown("---")
st.sidebar.subheader("Date Range")
min_date = df["observation_month"].min().to_pydatetime()
max_date = df["observation_month"].max().to_pydatetime()
date_range = st.sidebar.slider(
    "Select range",
    min_value=min_date,
    max_value=max_date,
    value=(pd.Timestamp("2015-01-01").to_pydatetime(), max_date),
    format="YYYY-MM",
)
filtered = df[(df["observation_month"] >= date_range[0]) & (df["observation_month"] <= date_range[1])]


# ============================================
# Page 1: Economic Overview
# ============================================
if page == "Economic Overview":
    st.title("Economic Overview")
    st.markdown("Key U.S. economic indicators from the FRED API, transformed through a dbt pipeline.")

    # KPI cards
    # Defensive formatting: any indicator can be NULL in the latest row
    # if FRED hasn't published the most recent period yet (common for
    # UMCSENT, which is typically released with a ~2-week lag).
    if not summary.empty:
        row = summary.iloc[0]

        def fmt_value(val, spec=".1f", suffix="%"):
            """Format a numeric value or return 'N/A' if null."""
            if pd.isna(val):
                return "N/A"
            return f"{val:{spec}}{suffix}"

        def fmt_delta(val, spec="+.1f", suffix="pp"):
            """Format a delta value or return None (Streamlit hides null deltas)."""
            if pd.isna(val):
                return None
            return f"{val:{spec}}{suffix}"

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "Unemployment Rate",
            fmt_value(row["unemployment_current"]),
            fmt_delta(row["unemployment_mom_change"]),
            delta_color="inverse",
        )
        c2.metric(
            "Inflation (YoY)",
            fmt_value(row["inflation_current"]),
            fmt_delta(row["inflation_mom_change"]),
            delta_color="inverse",
        )
        c3.metric(
            "Fed Funds Rate",
            fmt_value(row["fed_funds_current"], spec=".2f"),
            fmt_delta(row["fed_funds_mom_change"], spec="+.2f"),
        )
        c4.metric(
            "Consumer Sentiment",
            fmt_value(row["sentiment_current"], suffix=""),
            fmt_delta(row["sentiment_mom_change"], suffix=""),
        )

    st.markdown("---")

    # Indicator charts — 2x2 grid
    col1, col2 = st.columns(2)

    with col1:
        fig = px.line(filtered, x="observation_month", y="unemployment_rate",
                      title="Unemployment Rate (%)",
                      labels={"observation_month": "", "unemployment_rate": "%"})
        fig.update_layout(height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.line(filtered, x="observation_month", y="inflation_rate_yoy",
                      title="Inflation Rate — Year over Year (%)",
                      labels={"observation_month": "", "inflation_rate_yoy": "%"})
        fig.update_layout(height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        fig = px.line(filtered, x="observation_month", y="fed_funds_rate",
                      title="Federal Funds Rate (%)",
                      labels={"observation_month": "", "fed_funds_rate": "%"})
        fig.update_layout(height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = px.line(filtered, x="observation_month", y="consumer_sentiment",
                      title="Consumer Sentiment Index",
                      labels={"observation_month": "", "consumer_sentiment": "Index"})
        fig.update_layout(height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)


# ============================================
# Page 2: Recession Risk Monitor
# ============================================
elif page == "Recession Risk Monitor":
    st.title("Recession Risk Monitor")
    st.markdown("Composite recession risk scoring based on 5 established economic signals.")

    # Current risk level
    latest = df.dropna(subset=["recession_risk_level"]).iloc[-1]
    risk = latest["recession_risk_level"]
    signal_count = int(latest["recession_signal_count"])

    risk_colors = {"LOW": "🟢", "MODERATE": "🟡", "ELEVATED": "🟠", "HIGH": "🔴"}
    st.markdown(f"### Current Risk Level: {risk_colors.get(risk, '⚪')} **{risk}** ({signal_count}/5 signals active)")

    st.markdown("---")

    # Individual signals
    # Emoji carries the main signal; status word is a small supporting label.
    # Detail value sits beneath in a muted caption.
    st.subheader("Signal Status")

    def signal_status(val):
        """Return (emoji, label) pair for a signal value. Handles NaN defensively."""
        if pd.isna(val):
            return ("⚪", "No Data")
        return ("🔴", "Active") if val == 1 else ("🟢", "Inactive")

    def fmt_metric(val, spec=".2f", suffix=""):
        """Format a numeric metric or return 'N/A' if null."""
        if pd.isna(val):
            return "N/A"
        return f"{val:{spec}}{suffix}"

    signals = [
        {
            "label": "Sahm Rule",
            "value": latest.get("signal_sahm_rule"),
            "detail": f"Value: {fmt_metric(latest.get('sahm_rule_value'))}",
        },
        {
            "label": "Yield Curve",
            "value": latest.get("signal_yield_curve_inverted"),
            "detail": f"Spread: {fmt_metric(latest.get('treasury_spread_10y2y'))}",
        },
        {
            "label": "High Inflation",
            "value": latest.get("signal_high_inflation"),
            "detail": f"YoY: {fmt_metric(latest.get('inflation_rate_yoy'), suffix='%')}",
        },
        {
            "label": "Low Sentiment",
            "value": latest.get("signal_low_sentiment"),
            "detail": f"Index: {fmt_metric(latest.get('consumer_sentiment'), spec='.1f')}",
        },
        {
            "label": "Fed Tightening",
            "value": latest.get("signal_fed_tightening"),
            "detail": f"YoY Δ: {fmt_metric(latest.get('fed_funds_rate_yoy_change'), suffix='pp')}",
        },
    ]

    cols = st.columns(5)
    for col, sig in zip(cols, signals):
        emoji, label = signal_status(sig["value"])
        with col:
            st.markdown(
                f"""
                <div style="text-align: left; padding: 0.25rem 0;">
                    <div style="font-weight: 600; font-size: 0.95rem; margin-bottom: 0.25rem;">
                        {sig['label']}
                    </div>
                    <div style="font-size: 0.5rem; line-height: 1.2;">
                        {emoji} <span style="font-size: 1rem; font-weight: 500;">{label}</span>
                    </div>
                    <div style="color: #888; font-size: 0.85rem; margin-top: 0.4rem;">
                        {sig['detail']}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Historical signal count chart
    st.subheader("Historical Recession Signal Count")
    fig = px.area(filtered, x="observation_month", y="recession_signal_count",
                  title="Number of Active Recession Signals Over Time",
                  labels={"observation_month": "", "recession_signal_count": "Active Signals"},
                  color_discrete_sequence=["#ff6b6b"])
    fig.update_layout(height=400, margin=dict(t=40, b=20))
    fig.add_hline(y=3, line_dash="dash", line_color="red",
                  annotation_text="HIGH risk threshold")
    fig.add_hline(y=2, line_dash="dash", line_color="orange",
                  annotation_text="ELEVATED risk threshold")
    st.plotly_chart(fig, use_container_width=True)

    # Sahm Rule chart
    st.subheader("Sahm Rule Indicator")
    sahm_data = filtered.dropna(subset=["sahm_rule_value"])
    if not sahm_data.empty:
        fig = px.line(sahm_data, x="observation_month", y="sahm_rule_value",
                      title="Sahm Rule Value (≥0.50 = Recession Signal)",
                      labels={"observation_month": "", "sahm_rule_value": "Sahm Value"})
        fig.add_hline(y=0.5, line_dash="dash", line_color="red",
                      annotation_text="Trigger threshold (0.50)")
        fig.update_layout(height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)


# ============================================
# Page 3: Indicator Deep Dive
# ============================================
elif page == "Indicator Deep Dive":
    st.title("Indicator Deep Dive")

    indicator = st.selectbox("Select Indicator", [
        "Unemployment Rate",
        "CPI / Inflation",
        "Federal Funds Rate",
        "Consumer Sentiment",
        "Treasury Yield Spread",
        "GDP",
    ])

    if indicator == "Unemployment Rate":
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                            subplot_titles=("Unemployment Rate (%)", "3-Month vs 12-Month Rolling Average"))
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["unemployment_rate"],
                                 name="Monthly", line=dict(color="#636EFA")), row=1, col=1)
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["unemployment_rate_3m_avg"],
                                 name="3M Avg", line=dict(dash="dash", color="#EF553B")), row=2, col=1)
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["unemployment_rate_12m_avg"],
                                 name="12M Avg", line=dict(dash="dot", color="#00CC96")), row=2, col=1)
        fig.update_layout(height=600, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    elif indicator == "CPI / Inflation":
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                            subplot_titles=("CPI Index Level", "Year-over-Year Inflation Rate (%)"))
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["cpi_index"],
                                 name="CPI Index", line=dict(color="#636EFA")), row=1, col=1)
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["inflation_rate_yoy"],
                                 name="YoY %", line=dict(color="#EF553B")), row=2, col=1)
        fig.add_hline(y=2, row=2, col=1, line_dash="dash", line_color="green",
                      annotation_text="Fed target (2%)")
        fig.update_layout(height=600, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    elif indicator == "Federal Funds Rate":
        fig = px.line(filtered, x="observation_month", y="fed_funds_rate",
                      title="Federal Funds Rate (%)")
        fig.update_layout(height=400, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.bar(filtered, x="observation_month", y="fed_funds_rate_yoy_change",
                      title="Fed Funds Rate — Year-over-Year Change (pp)",
                      color_discrete_sequence=["#AB63FA"])
        fig2.update_layout(height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    elif indicator == "Consumer Sentiment":
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                            subplot_titles=("Consumer Sentiment Index", "Month-over-Month Change"))
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["consumer_sentiment"],
                                 name="Sentiment", line=dict(color="#636EFA")), row=1, col=1)
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["sentiment_3m_avg"],
                                 name="3M Avg", line=dict(dash="dash", color="#EF553B")), row=1, col=1)
        fig.add_trace(go.Bar(x=filtered["observation_month"], y=filtered["sentiment_mom_change"],
                             name="MoM Change"), row=2, col=1)
        fig.add_hline(y=60, row=1, col=1, line_dash="dash", line_color="red",
                      annotation_text="Low sentiment threshold")
        fig.update_layout(height=600, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    elif indicator == "Treasury Yield Spread":
        fig = px.area(filtered, x="observation_month", y="treasury_spread_10y2y",
                      title="10Y-2Y Treasury Yield Spread",
                      color_discrete_sequence=["#19D3F3"])
        fig.add_hline(y=0, line_dash="dash", line_color="red",
                      annotation_text="Inversion line (recession signal)")
        fig.update_layout(height=400, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    elif indicator == "GDP":
        gdp_data = filtered.dropna(subset=["gdp_qoq_growth_rate"])
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                            subplot_titles=("GDP (Billions $)", "Quarter-over-Quarter Growth Rate (%)"))
        fig.add_trace(go.Scatter(x=filtered["observation_month"], y=filtered["gdp"],
                                 name="GDP", line=dict(color="#636EFA")), row=1, col=1)
        fig.add_trace(go.Bar(x=gdp_data["observation_month"], y=gdp_data["gdp_qoq_growth_rate"],
                             name="QoQ Growth %"), row=2, col=1)
        fig.add_hline(y=0, row=2, col=1, line_dash="dash", line_color="red")
        fig.update_layout(height=600, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)


# ============================================
# Page 4: Data Quality
# ============================================
elif page == "Data Quality":
    st.title("Data Quality & Pipeline Status")

    # Last refresh info
    latest_month = df["observation_month"].max()
    st.info(f"📅 Latest data month: **{latest_month.strftime('%B %Y')}**")

    # Observation counts per series
    st.subheader("Observation Counts by Series")
    counts = df.melt(
        id_vars=["observation_month"],
        value_vars=["unemployment_rate", "cpi_index", "fed_funds_rate",
                     "consumer_sentiment", "gdp", "treasury_spread_10y2y"],
    ).dropna(subset=["value"]).groupby("variable").size().reset_index(name="count")
    counts.columns = ["Indicator", "Observation Count"]
    st.dataframe(counts, use_container_width=True, hide_index=True)

    # Ingestion log
    st.subheader("Recent Ingestion Runs")
    log_df = load_ingestion_log()
    if not log_df.empty:
        st.dataframe(log_df, use_container_width=True, hide_index=True)
    else:
        st.warning("No ingestion logs found.")

    # Null analysis
    st.subheader("Data Completeness (Last 24 Months)")
    recent = df[df["observation_month"] >= df["observation_month"].max() - pd.DateOffset(months=24)]
    null_counts = recent[["observation_month", "unemployment_rate", "cpi_index",
                          "fed_funds_rate", "consumer_sentiment", "gdp",
                          "treasury_spread_10y2y"]].isnull().sum()
    null_df = pd.DataFrame({"Column": null_counts.index, "Missing Values": null_counts.values})
    st.dataframe(null_df, use_container_width=True, hide_index=True)


# ============================================
# Footer
# ============================================
st.sidebar.markdown("---")
st.sidebar.markdown(
    "Built with Airflow + dbt + Snowflake + Streamlit  \n"
    "Data source: [FRED API](https://fred.stlouisfed.org/)"
)