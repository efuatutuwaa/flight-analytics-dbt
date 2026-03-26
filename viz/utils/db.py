import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from databricks import sql

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../../.env"))

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]


@st.cache_data
def query(sql_query: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            cols = [d[0] for d in cursor.description]
            return pd.DataFrame(cursor.fetchall(), columns=cols)
