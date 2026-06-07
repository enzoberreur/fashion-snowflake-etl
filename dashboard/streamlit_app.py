"""Monogram Paris - pipeline insights dashboard (Streamlit on Snowflake).

Two views:
  - Business insights: KPIs + charts from the MARTS star schema (fact_sales + dims).
  - Real-time stream: live metrics from INGEST.STREAM_SALES (updated by the
    Snowpipe Streaming consumer): throughput, end-to-end latency, freshness,
    per-partition counts, and the most recent events.

Run:  streamlit run dashboard/streamlit_app.py
Needs the same .env / key-pair as the rest of the pipeline.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from monogram_etl.config.snowflake import SnowflakeConnection  # noqa: E402

st.set_page_config(page_title="Monogram Paris - Pipeline Insights", page_icon="👑", layout="wide")


@st.cache_resource
def _connection() -> SnowflakeConnection:
    return SnowflakeConnection(query_tag="streamlit-dashboard")


@st.cache_data(ttl=10)
def q(sql: str) -> pd.DataFrame:
    cur = _connection().connection.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


st.title("👑 Monogram Paris - Real-Time Pipeline Insights")
st.caption("Snowflake warehouse (MARTS star schema) + live Snowpipe Streaming. Bloc 3.")
if st.button("🔄 Refresh"):
    st.cache_data.clear()

business, realtime = st.tabs(["📊 Business insights (MARTS)", "⚡ Real-time stream"])

with business:
    kpi = q(
        "select count(*) sales, sum(gross_amount) revenue, avg(gross_amount) aov, "
        "sum(return_count) returns from MARTS.FACT_SALES"
    ).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sales", f"{int(kpi.SALES):,}")
    c2.metric("Revenue", f"€{kpi.REVENUE:,.0f}")
    c3.metric("Avg order value", f"€{kpi.AOV:,.0f}")
    c4.metric("Return rate", f"{(kpi.RETURNS / max(1, kpi.SALES)) * 100:.1f}%")

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by channel")
        df = q("select channel, sum(gross_amount) revenue from MARTS.FACT_SALES group by 1 order by 2 desc")
        st.bar_chart(df, x="CHANNEL", y="REVENUE", color="#00A651")
    with right:
        st.subheader("Revenue by store country")
        df = q(
            "select s.country, sum(f.gross_amount) revenue from MARTS.FACT_SALES f "
            "join MARTS.DIM_STORE s on f.store_sk = s.store_sk group by 1 order by 2 desc"
        )
        st.bar_chart(df, x="COUNTRY", y="REVENUE", color="#00A651")

    st.subheader("Monthly revenue trend")
    df = q(
        "select date_trunc('month', sale_date) month, sum(gross_amount) revenue "
        "from MARTS.FACT_SALES group by 1 order by 1"
    )
    st.line_chart(df, x="MONTH", y="REVENUE", color="#00A651")

    left, right = st.columns(2)
    with left:
        st.subheader("Top brands by revenue")
        df = q(
            "select p.brand, sum(f.gross_amount) revenue, sum(f.quantity) units "
            "from MARTS.FACT_SALES f join MARTS.DIM_PRODUCT p on f.product_sk = p.product_sk "
            "group by 1 order by 2 desc limit 10"
        )
        st.dataframe(df, hide_index=True, use_container_width=True)
    with right:
        st.subheader("Customer value tiers")
        df = q(
            "select value_tier, count(*) customers, sum(observed_revenue) revenue "
            "from MARTS.DIM_CUSTOMER group by 1 order by 2 desc"
        )
        st.dataframe(df, hide_index=True, use_container_width=True)

with realtime:
    s = q(
        "select count(*) events, "
        "round(avg(datediff('millisecond', event_ts, ingested_at)) / 1000.0, 2) avg_latency_s, "
        "round(max(datediff('millisecond', event_ts, ingested_at)) / 1000.0, 2) max_latency_s, "
        "count(distinct kafka_partition) partitions, "
        "timestampdiff('second', max(ingested_at), sysdate()) seconds_since_last "
        "from INGEST.STREAM_SALES"
    ).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Streamed events", f"{int(s.EVENTS):,}")
    c2.metric("Avg latency", f"{s.AVG_LATENCY_S:.2f}s" if pd.notna(s.AVG_LATENCY_S) else "-")
    c3.metric("Kafka partitions", int(s.PARTITIONS) if pd.notna(s.PARTITIONS) else 0)
    secs = int(s.SECONDS_SINCE_LAST) if pd.notna(s.SECONDS_SINCE_LAST) else None
    c4.metric("Seconds since last event", secs if secs is not None else "-",
              delta="FRESH" if (secs is not None and secs <= 60) else "STALE",
              delta_color="normal" if (secs is not None and secs <= 60) else "inverse")

    left, right = st.columns([1, 2])
    with left:
        st.subheader("Events per partition")
        df = q("select kafka_partition partition, count(*) events from INGEST.STREAM_SALES group by 1 order by 1")
        st.bar_chart(df, x="PARTITION", y="EVENTS", color="#00A651")
    with right:
        st.subheader("Latest streamed sales")
        df = q(
            "select event_ts, store_id, channel, product_name, total_amount, "
            "kafka_partition, kafka_offset from INGEST.STREAM_SALES order by ingested_at desc limit 20"
        )
        st.dataframe(df, hide_index=True, use_container_width=True)

    st.caption("Start the stream with: docker compose up -d  ·  monogram-stream-produce  ·  monogram-stream-consume")
