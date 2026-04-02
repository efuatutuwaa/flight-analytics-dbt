import streamlit as st
import plotly.express as px
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from utils.db import query
from utils.theme import apply_theme, BLUE_SCALE, PRIMARY

st.set_page_config(page_title="Codeshare Complexity", page_icon="🔗", layout="wide")

st.title("🔗 Codeshare Complexity")
st.caption("How do marketing airlines and operating airlines interact?")
st.divider()

df = query("SELECT * FROM workspace.mart_models.mart_codeshare_complexity")

# sidebar filter
with st.sidebar:
    st.header("Filters")
    selected_operator = st.multiselect(
        "Operating Airline",
        options=sorted(df["operating_airline_name"].dropna().unique()),
        default=[]
    )

filtered = df[df["operating_airline_name"].isin(selected_operator)] if selected_operator else df

# kpi row
c1, c2, c3 = st.columns(3)
c1.metric("Partnerships", len(filtered))
c2.metric("Total Codeshare Flights", f"{filtered['total_codeshare_flights'].sum():,}")
c3.metric("Avg Codeshare Rate", f"{filtered['codeshare_rate'].mean():.1%}")

st.divider()

# chart 1 — top partnerships by total codeshare flights
st.subheader("Biggest Codeshare Partnerships")
st.caption("Top 20 operating + marketing airline pairs by total flights")

top = filtered.nlargest(20, "total_codeshare_flights").copy()
top["partnership"] = top["operating_airline_name"] + " → " + top["marketing_airline_name"]

fig1 = px.bar(
    top,
    x="total_codeshare_flights",
    y="partnership",
    orientation="h",
    color="codeshare_rate",
    color_continuous_scale=BLUE_SCALE,
    labels={
        "total_codeshare_flights": "Total Flights",
        "partnership": "Partnership",
        "codeshare_rate": "Codeshare Rate"
    }
)
fig1.update_layout(yaxis={"categoryorder": "total ascending"})
apply_theme(fig1, height=520)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# chart 2 — codeshare rate vs routes shared scatter
st.subheader("Codeshare Rate vs Routes Shared")
st.caption("Each dot = one partnership · Size = total flights · Color = codeshare rate")

fig2 = px.scatter(
    filtered,
    x="total_routes_shared",
    y="codeshare_rate",
    size="total_codeshare_flights",
    color="codeshare_rate",
    hover_name="operating_airline_name",
    hover_data={
        "marketing_airline_name": True,
        "total_codeshare_flights": True,
        "total_true_codeshares": True,
        "codeshare_rate": ":.0%"
    },
    color_continuous_scale=BLUE_SCALE,
    labels={
        "total_routes_shared": "Routes Shared",
        "codeshare_rate": "Codeshare Rate",
        "total_codeshare_flights": "Total Flights"
    }
)
fig2.update_layout(yaxis_tickformat=".0%")
apply_theme(fig2)
st.plotly_chart(fig2, use_container_width=True)
