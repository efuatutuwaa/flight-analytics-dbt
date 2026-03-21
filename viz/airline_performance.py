import os
import streamlit as st
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from databricks import sql

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

st.set_page_config(page_title="Airline Performance", layout="wide")
st.title("Airline Performance Dashboard")


@st.cache_data
def load_data():
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM workspace.mart_models.mart_airline_performance")
            cols = [d[0] for d in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=cols)


df = load_data()

# sidebar filter
airlines = st.sidebar.multiselect(
    "Filter Airlines",
    options=sorted(df["airline_name"].dropna().unique()),
    default=df.nlargest(10, "total_flights")["airline_name"].tolist()
)

filtered = df[df["airline_name"].isin(airlines)] if airlines else df

# row 1
col1, col2, col3 = st.columns(3)
col1.metric("Airlines", len(filtered))
col2.metric("Total Flights", f"{filtered['total_flights'].sum():,}")
col3.metric("Avg Cancellation Rate", f"{filtered['cancellation_rate'].mean():.1%}")

st.divider()

# bar chart — busiest airlines
st.subheader("Busiest Airlines by Total Flights")
fig1 = px.bar(
    filtered.sort_values("total_flights", ascending=False),
    x="airline_name", y="total_flights",
    labels={"airline_name": "Airline", "total_flights": "Total Flights"}
)
fig1.update_traces(marker_color="#1f4e79")
fig1.update_layout(showlegend=False, xaxis_tickangle=-45)
st.plotly_chart(fig1, use_container_width=True)

# scatter — delay recovery
st.subheader("Departure vs Arrival Delay (negative = arriving early)")
fig2 = px.scatter(
    filtered,
    x="avg_departure_delay_minutes",
    y="avg_arrival_delay_minutes",
    text="airline_name",
    size="total_flights",
    color="cancellation_rate",
    color_continuous_scale="Blues",
    labels={
        "avg_departure_delay_minutes": "Avg Departure Delay (mins)",
        "avg_arrival_delay_minutes": "Avg Arrival Delay (mins)",
        "cancellation_rate": "Cancellation Rate"
    }
)
fig2.update_traces(textposition="top center")
fig2.add_hline(y=0, line_dash="dash", line_color="gray")
fig2.add_vline(x=0, line_dash="dash", line_color="gray")
st.plotly_chart(fig2, use_container_width=True)

# on time vs delayed
st.subheader("On-Time vs Delayed Departures")
fig3 = px.bar(
    filtered.sort_values("total_flights", ascending=False),
    x="airline_name",
    y=["on_time_departures", "delayed_departures"],
    barmode="stack",
    labels={"airline_name": "Airline", "value": "Flights", "variable": "Status"},
    color_discrete_map={"on_time_departures": "#1f4e79", "delayed_departures": "#a8c4e0"}
)
fig3.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig3, use_container_width=True)
