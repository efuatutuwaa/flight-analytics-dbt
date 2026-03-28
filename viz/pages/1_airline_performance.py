import streamlit as st
import plotly.express as px
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.db import query
from utils.theme import apply_theme, PERF_SCALE, BLUE_SCALE, PRIMARY

st.set_page_config(page_title="Airline Performance", page_icon="🏷️", layout="wide")

st.title("🏷️ Airline Performance")
st.caption("Which airlines operate the most flights and which perform best?")
st.divider()

df = query("SELECT * FROM workspace.mart_models.mart_airline_performance")

# sidebar filter
with st.sidebar:
    st.header("Filters")
    airline_types = st.multiselect(
        "Airline Type",
        options=sorted(df["airline_type"].dropna().unique()),
        default=["scheduled", "scheduled,charter", "scheduled,cargo"]
    )
    type_filtered = df[df["airline_type"].isin(airline_types)] if airline_types else df

    selected = st.multiselect(
        "Airlines",
        options=sorted(type_filtered["airline_name"].dropna().unique()),
        default=type_filtered.nlargest(10, "total_flights")["airline_name"].tolist()
    )

filtered = type_filtered[type_filtered["airline_name"].isin(selected)] if selected else type_filtered

# kpi row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Airlines", len(filtered))
c2.metric("Total Flights", f"{filtered['total_flights'].sum():,}")
c3.metric("Avg On-Time Departure Rate",
    f"{(filtered['on_time_departures'].sum() / (filtered['on_time_departures'].sum() + filtered['delayed_departures'].sum())):.1%}"
)
c4.metric("Avg Cancellation Rate",
    f"{(filtered['cancelled_flights'].sum() / filtered['total_flights'].sum()):.1%}"
)

st.divider()

# chart 1 — on-time departure rate by airline
st.subheader("On-Time Departure Rate by Airline")
rate_df = filtered.copy()
rate_df["on_time_rate"] = rate_df["on_time_departures"] / (
    rate_df["on_time_departures"] + rate_df["delayed_departures"]
)
rate_df = rate_df.sort_values("on_time_rate", ascending=False)

fig1 = px.bar(
    rate_df,
    x="airline_name",
    y="on_time_rate",
    labels={"airline_name": "Airline", "on_time_rate": "On-Time Rate"},
    color="on_time_rate",
    color_continuous_scale=PERF_SCALE,
)
fig1.update_layout(xaxis_tickangle=-45, yaxis_tickformat=".0%")
apply_theme(fig1)
st.plotly_chart(fig1, use_container_width=True)

# chart 2 — departure vs arrival delay scatter
st.subheader("Departure vs Arrival Delay")
st.caption("Each dot = one airline · Hover for details · Color = cancellation rate")
fig2 = px.scatter(
    filtered,
    x="avg_departure_delay_minutes",
    y="avg_arrival_delay_minutes",
    hover_name="airline_name",
    hover_data={"total_flights": True, "cancellation_rate": ":.2%"},
    color="cancellation_rate",
    color_continuous_scale=BLUE_SCALE,
    labels={
        "avg_departure_delay_minutes": "Avg Departure Delay (mins)",
        "avg_arrival_delay_minutes": "Avg Arrival Delay (mins)",
        "cancellation_rate": "Cancellation Rate"
    }
)
fig2.update_traces(marker=dict(size=14))
fig2.add_hline(y=0, line_dash="dash", line_color="#D1D5DB")
fig2.add_vline(x=0, line_dash="dash", line_color="#D1D5DB")
apply_theme(fig2)
st.plotly_chart(fig2, use_container_width=True)
