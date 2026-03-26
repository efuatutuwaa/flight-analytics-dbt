import streamlit as st

st.set_page_config(
    page_title="Flight Analytics",
    page_icon="✈️",
    layout="wide"
)

st.title("✈️ Flight Analytics")
st.markdown("An end-to-end analytics engineering project modelling real-world flight data from the AviationStack API using **dbt** and **Databricks**.")

st.divider()

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🏷️ Airline Performance")
    st.markdown("Carrier-level flight volumes, on-time performance, and cancellation rates.")
    st.page_link("pages/1_airline_performance.py", label="View Dashboard →")

with col2:
    st.markdown("### 🗺️ Route Performance")
    st.markdown("Route efficiency metrics — delays, cancellations, and competition by route.")
    st.page_link("pages/2_route_performance.py", label="View Dashboard →")

with col3:
    st.markdown("### 🏢 Airport Operations")
    st.markdown("Airport traffic volumes and departure/arrival performance by airport.")
    st.page_link("pages/3_airport_operations.py", label="View Dashboard →")

st.divider()

st.markdown(
    """
    **Stack:** AviationStack API · dbt Core · Databricks · Streamlit · Plotly

    **Source:** [github.com/efuatutuwaa/flight-analytics-dbt](https://github.com/efuatutuwaa/flight-analytics-dbt)
    """
)
