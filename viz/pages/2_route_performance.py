import streamlit as st
import plotly.express as px
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.db import query

st.set_page_config(page_title="Route Performance", page_icon="🗺️", layout="wide")

st.title("🗺️ Route Performance")
st.caption("Which routes are efficient and which are problematic?")
st.divider()

df = query("SELECT * FROM workspace.mart_models.mart_route_performance")

# sidebar filter
with st.sidebar:
    st.header("Filters")
    route_type = st.radio("Route Type", ["All", "International", "Domestic"])
    top_n = st.slider("Top N Routes", min_value=5, max_value=30, value=15)

filtered = df.copy()
if route_type == "International":
    filtered = filtered[filtered["is_international"] == True]
elif route_type == "Domestic":
    filtered = filtered[filtered["is_international"] == False]

# kpi row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Routes", f"{len(filtered):,}")
c2.metric("Total Flights", f"{filtered['total_flights'].sum():,}")
c3.metric("Avg Cancellation Rate",
    f"{(filtered['cancelled_flights'].sum() / filtered['total_flights'].sum()):.1%}"
    if filtered['total_flights'].sum() > 0 else "N/A"
)
c4.metric("International Routes",
    f"{filtered['is_international'].sum():,}"
)

st.divider()

# chart 1 — top routes by avg departure delay
st.subheader(f"Top {top_n} Most Delayed Routes")
delay_df = filtered[filtered["total_flights"] > 0].nlargest(top_n, "avg_departure_delay_minutes")
fig1 = px.bar(
    delay_df,
    x="avg_departure_delay_minutes",
    y="route_label",
    orientation="h",
    color="avg_departure_delay_minutes",
    color_continuous_scale=["#1f4e79", "#f39c12", "#e74c3c"],
    labels={"avg_departure_delay_minutes": "Avg Departure Delay (mins)", "route_label": "Route"}
)
fig1.update_layout(
    coloraxis_showscale=False,
    yaxis={"categoryorder": "total ascending"},
    plot_bgcolor="white",
    paper_bgcolor="white"
)
st.plotly_chart(fig1, use_container_width=True)

# chart 2 — treemap of route network
st.subheader("Route Network — Traffic & Delay")
st.caption("Block size = total flights · Color = avg departure delay (red = more delayed)")
treemap_df = filtered[
    filtered["total_flights"] > 0 &
    filtered["route_label"].notna()
].nlargest(60, "total_flights")

fig2 = px.treemap(
    treemap_df,
    path=["route_label"],
    values="total_flights",
    color="avg_departure_delay_minutes",
    color_continuous_scale=["#1f4e79", "#f39c12", "#e74c3c"],
    hover_data={"avg_arrival_delay_minutes": True, "cancellation_rate": ":.2%"},
    labels={
        "total_flights": "Total Flights",
        "avg_departure_delay_minutes": "Avg Dep Delay (mins)",
        "avg_arrival_delay_minutes": "Avg Arr Delay (mins)"
    }
)
fig2.update_layout(margin=dict(t=10, l=0, r=0, b=0))
st.plotly_chart(fig2, use_container_width=True)
