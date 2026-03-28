import streamlit as st
import plotly.express as px
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.db import query
from utils.theme import apply_theme, PERF_SCALE, BLUE_SCALE

st.set_page_config(page_title="Airport Operations", page_icon="🏢", layout="wide")

st.title("🏢 Airport Operations")
st.caption("Which airports are busiest or causing operational bottlenecks?")
st.divider()

df = query("SELECT * FROM workspace.mart_models.mart_airport_operations")

# sidebar filter
with st.sidebar:
    st.header("Filters")
    continents = ["All"] + sorted(df["country_continent"].dropna().unique().tolist())
    selected_continent = st.selectbox("Continent", continents)
    top_n = st.slider("Top N Airports", min_value=5, max_value=30, value=15)

filtered = df.copy()
if selected_continent != "All":
    filtered = filtered[filtered["country_continent"] == selected_continent]

# kpi row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Airports", f"{len(filtered):,}")
c2.metric("Total Movements", f"{filtered['total_movements'].sum():,}")
busiest = filtered.nlargest(1, "total_movements")
c3.metric("Busiest Airport",
    busiest["airport_iata_code"].values[0] if len(busiest) > 0 else "N/A"
)
c4.metric("Avg On-Time Departure Rate",
    f"{(filtered['on_time_departures'].sum() / (filtered['on_time_departures'].sum() + filtered['delayed_departures'].sum())):.1%}"
    if (filtered['on_time_departures'].sum() + filtered['delayed_departures'].sum()) > 0 else "N/A"
)

st.divider()

# chart 1 — busiest airports
st.subheader(f"Top {top_n} Busiest Airports")
busy_df = filtered.nlargest(top_n, "total_movements")
fig1 = px.bar(
    busy_df,
    x="total_movements",
    y="airport_iata_code",
    orientation="h",
    color="total_movements",
    color_continuous_scale=BLUE_SCALE,
    hover_data=["airport_name", "city_name", "country_name"],
    labels={"total_movements": "Total Movements", "airport_iata_code": "Airport"}
)
fig1.update_layout(yaxis={"categoryorder": "total ascending"})
apply_theme(fig1, height=480)
st.plotly_chart(fig1, use_container_width=True)

# chart 2 — on-time departure rate
st.subheader(f"On-Time Departure Rate — Top {top_n} Busiest Airports")
rate_df = busy_df.copy()
rate_df["on_time_departure_rate"] = rate_df["on_time_departures"] / (
    rate_df["on_time_departures"] + rate_df["delayed_departures"]
)
rate_df = rate_df.sort_values("on_time_departure_rate", ascending=False)

fig2 = px.bar(
    rate_df,
    x="airport_iata_code",
    y="on_time_departure_rate",
    color="on_time_departure_rate",
    color_continuous_scale=PERF_SCALE,
    hover_data=["airport_name", "city_name"],
    labels={"airport_iata_code": "Airport", "on_time_departure_rate": "On-Time Rate"}
)
fig2.update_layout(yaxis_tickformat=".0%")
apply_theme(fig2)
st.plotly_chart(fig2, use_container_width=True)
