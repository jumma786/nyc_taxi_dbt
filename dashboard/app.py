"""
NYC Taxi Analytics — Streamlit dashboard.

Reads the dbt marts (agg_daily_revenue, fct_trips) directly from the local
DuckDB file and presents them as an interactive BI layer: KPI tiles, revenue
by borough, trips by hour of day, and a daily revenue trend.

Run:
    dbt build --profiles-dir .        # build the marts first
    streamlit run dashboard/app.py

The DuckDB path defaults to nyc_taxi.duckdb at the project root and can be
overridden with the NYC_TAXI_DUCKDB environment variable.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# --- config -----------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = os.environ.get("NYC_TAXI_DUCKDB", str(PROJECT_ROOT / "nyc_taxi.duckdb"))

# The default download is 2024 Jan–Mar. Raw TLC data carries a few rows with
# garbage timestamps (years like 2002/2009), so default the view to the
# intended window instead of letting outliers set the axis range.
DEFAULT_START = date(2024, 1, 1)
DEFAULT_END = date(2024, 3, 31)

# Validated, colorblind-safe categorical palette (dataviz skill reference
# instance). Boroughs are assigned these hues in fixed order — never cycled.
BOROUGH_COLORS = {
    "Manhattan": "#2a78d6",      # blue
    "Queens": "#1baf7a",         # aqua
    "Brooklyn": "#eda100",       # yellow
    "Bronx": "#4a3aa7",          # violet
    "Staten Island": "#e34948",  # red
    "EWR": "#e87ba4",            # magenta
    "N/A": "#898781",            # muted
    "Unknown": "#b0aea8",        # muted-light
}
PRIMARY = "#2a78d6"
INK_MUTED = "#898781"

st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide",
)


# --- data access ------------------------------------------------------------
#
# The app runs in two modes from one code path, both backed by DuckDB:
#   local  — the full dbt-built warehouse (nyc_taxi.duckdb) is present.
#   cloud  — no warehouse file (e.g. Streamlit Community Cloud); read the
#            committed pre-aggregated marts in dashboard/data/*.parquet.
# Either way we expose two views, `daily_src` and `hourly_src`, so every
# query below is identical regardless of backend.

DASH_DATA = Path(__file__).resolve().parent / "data"


@st.cache_resource
def _connect() -> duckdb.DuckDBPyConnection:
    # Always an in-memory connection so we can define views; the data backend
    # is either the attached read-only warehouse (local) or parquet (cloud).
    con = duckdb.connect(":memory:")

    if Path(DB_PATH).exists():
        con.execute(f"attach '{DB_PATH}' as w (read_only)")
        con.execute("create view daily_src as select * from w.main.agg_daily_revenue")
        con.execute(
            "create view hourly_src as "
            "select pickup_date, pickup_borough, pickup_hour, count(*) as trip_count "
            "from w.main.fct_trips group by 1, 2, 3"
        )
        return con

    daily_pq = DASH_DATA / "daily.parquet"
    hourly_pq = DASH_DATA / "hourly.parquet"
    if not (daily_pq.exists() and hourly_pq.exists()):
        st.error(
            "No data found. Either build the marts locally:\n"
            "    python scripts/download_data.py --year 2024 --months 1 2 3\n"
            "    dbt deps --profiles-dir . && dbt build --profiles-dir .\n"
            "or ship the pre-aggregated dashboard/data/*.parquet files."
        )
        st.stop()

    con.execute(f"create view daily_src as select * from read_parquet('{daily_pq.as_posix()}')")
    con.execute(f"create view hourly_src as select * from read_parquet('{hourly_pq.as_posix()}')")
    return con


@st.cache_data(show_spinner=False)
def load_daily(start: date, end: date, boroughs: tuple[str, ...]) -> pd.DataFrame:
    con = _connect()
    q = """
        select pickup_date, pickup_borough, trip_count, total_passengers,
               total_revenue, avg_fare, avg_distance, avg_duration_min, avg_tip_pct
        from daily_src
        where pickup_date >= ? and pickup_date < ?
          and pickup_borough in ?
        order by pickup_date
    """
    return con.execute(q, [start, end, list(boroughs)]).df()


@st.cache_data(show_spinner=False)
def load_hourly(start: date, end: date, boroughs: tuple[str, ...]) -> pd.DataFrame:
    con = _connect()
    q = """
        select pickup_hour, sum(trip_count) as trip_count
        from hourly_src
        where pickup_date >= ? and pickup_date < ?
          and pickup_borough in ?
        group by pickup_hour
        order by pickup_hour
    """
    return con.execute(q, [start, end, list(boroughs)]).df()


@st.cache_data(show_spinner=False)
def all_boroughs() -> list[str]:
    con = _connect()
    df = con.execute(
        "select distinct pickup_borough from daily_src "
        "where pickup_borough is not null order by 1"
    ).df()
    return df["pickup_borough"].tolist()


# --- chart helpers ----------------------------------------------------------

def _base_layout(fig: go.Figure, height: int = 320) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=8, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="system-ui, -apple-system, Segoe UI, sans-serif"),
        showlegend=False,
        hovermode="x unified",
    )
    fig.update_xaxes(showgrid=False, color=INK_MUTED)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(137,135,129,0.20)", color=INK_MUTED)
    return fig


def revenue_by_borough(daily: pd.DataFrame) -> go.Figure:
    g = (
        daily.groupby("pickup_borough", as_index=False)["total_revenue"]
        .sum()
        .sort_values("total_revenue", ascending=True)
    )
    fig = go.Figure(
        go.Bar(
            x=g["total_revenue"],
            y=g["pickup_borough"],
            orientation="h",
            marker_color=PRIMARY,
            marker_line_width=0,
            text=[f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K" for v in g["total_revenue"]],
            textposition="outside",
            hovertemplate="%{y}: $%{x:,.0f}<extra></extra>",
        )
    )
    fig = _base_layout(fig, height=300)
    fig.update_xaxes(showticklabels=False)
    fig.update_yaxes(showgrid=False)
    return fig


def trips_by_hour(hourly: pd.DataFrame) -> go.Figure:
    fig = go.Figure(
        go.Bar(
            x=hourly["pickup_hour"],
            y=hourly["trip_count"],
            marker_color=PRIMARY,
            marker_line_width=0,
            hovertemplate="%{x}:00 — %{y:,} trips<extra></extra>",
        )
    )
    fig = _base_layout(fig, height=300)
    fig.update_xaxes(title_text="hour of day", dtick=3)
    return fig


def daily_revenue_trend(daily: pd.DataFrame) -> go.Figure:
    # One line per borough, colored by entity (fixed palette). Top boroughs by
    # revenue are labeled directly; a legend backs up identity.
    fig = go.Figure()
    totals = (
        daily.groupby("pickup_borough")["total_revenue"].sum().sort_values(ascending=False)
    )
    for borough in totals.index:
        sub = daily[daily["pickup_borough"] == borough]
        fig.add_trace(
            go.Scatter(
                x=sub["pickup_date"],
                y=sub["total_revenue"],
                mode="lines",
                name=borough,
                line=dict(color=BOROUGH_COLORS.get(borough, INK_MUTED), width=2),
                hovertemplate=f"{borough}: $%{{y:,.0f}}<extra></extra>",
            )
        )
    fig = _base_layout(fig, height=360)
    fig.update_layout(showlegend=True, legend=dict(orientation="h", y=1.12, x=0))
    fig.update_yaxes(title_text="daily revenue ($)")
    return fig


# --- app --------------------------------------------------------------------

def kpi_row(daily: pd.DataFrame) -> None:
    total_rev = daily["total_revenue"].sum()
    total_trips = int(daily["trip_count"].sum())
    total_pax = int(daily["total_passengers"].sum())
    # trip-weighted average tip %
    avg_tip = (
        (daily["avg_tip_pct"] * daily["trip_count"]).sum() / total_trips
        if total_trips else 0
    )
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total revenue", f"${total_rev/1e6:,.1f}M")
    c2.metric("Total trips", f"{total_trips/1e6:,.2f}M")
    c3.metric("Total passengers", f"{total_pax/1e6:,.2f}M")
    c4.metric("Avg tip %", f"{avg_tip*100:,.1f}%")


def main() -> None:
    st.title("🚕 NYC Taxi Analytics")
    st.caption(
        "Yellow taxi trips (NYC TLC) transformed with dbt · served from DuckDB marts"
    )

    boroughs = all_boroughs()

    with st.sidebar:
        st.header("Filters")
        picked = st.date_input(
            "Pickup date range",
            value=(DEFAULT_START, DEFAULT_END),
            min_value=date(2024, 1, 1),
            max_value=date(2024, 4, 1),
        )
        # date_input returns a single date until both ends are chosen
        if isinstance(picked, (tuple, list)) and len(picked) == 2:
            start, end = picked
        else:
            st.info("Pick both ends of the date range.")
            st.stop()
        selected = st.multiselect(
            "Boroughs",
            options=boroughs,
            default=[b for b in boroughs if b not in ("N/A", "Unknown")],
        )
        st.divider()
        if Path(DB_PATH).exists():
            st.caption(f"Source: local DuckDB warehouse (`{Path(DB_PATH).name}`)")
        else:
            st.caption("Source: pre-aggregated dbt marts (`dashboard/data/*.parquet`)")

    if not selected:
        st.info("Select at least one borough.")
        st.stop()

    # end is exclusive in the query; add a day so the last day is included
    end_exclusive = pd.Timestamp(end) + pd.Timedelta(days=1)

    daily = load_daily(start, end_exclusive.date(), tuple(selected))
    hourly = load_hourly(start, end_exclusive.date(), tuple(selected))

    if daily.empty:
        st.warning("No trips in the selected range.")
        st.stop()

    kpi_row(daily)
    st.divider()

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by borough")
        st.plotly_chart(revenue_by_borough(daily), use_container_width=True)
    with right:
        st.subheader("Trips by hour of day")
        st.plotly_chart(trips_by_hour(hourly), use_container_width=True)

    st.subheader("Daily revenue trend")
    st.plotly_chart(daily_revenue_trend(daily), use_container_width=True)

    with st.expander("View underlying data (agg_daily_revenue)"):
        st.dataframe(daily, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
