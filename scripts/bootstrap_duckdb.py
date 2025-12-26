import duckdb
from pathlib import Path

# --- paths ---
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "flight_db.duckdb"

# --- connect to DuckDB ---
con = duckdb.connect(database=str(DB_PATH))
print(f"Connected to DuckDB database at {DB_PATH}")

# --- create tables ---
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

TABLES = {
    "airline": "airline.csv",
    "airports": "airports.csv",
    "arrivals": "arrivals.csv",
    "cities": "cities.csv",
    "codeshare": "codeshare.csv",
    "countries": "countries.csv",
    "departure": "departure.csv",
    "flight_details": "flight_details.csv",
    "routes": "routes.csv",
}

# --- load data into tables ---
for table_name, file_name in TABLES.items():
    file_path = DATA_DIR / file_name
    con.execute(f"""CREATE OR REPLACE TABLE raw.{table_name} AS
                    SELECT * FROM read_csv_auto('{file_path}');
                    """)
    
    print(f"Loaded data into table 'raw.{table_name}' from '{file_path}'")


# --- close connection ---
con.close()
print("Connection closed.")