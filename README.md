# ✈️ Flight Analytics — dbt Capstone Project

> An end-to-end analytics engineering project that models real-world flight data from the **AviationStack API** using **dbt** and **Databricks**. Built to demonstrate production-grade data modelling, transformation layering, and data quality practices.

---

## 📌 Project Overview

This project ingests flight data from the AviationStack API and transforms it through a structured multi-layer dbt pipeline into analysis-ready data marts. It answers five core business questions about the global aviation network:

| Domain | Business Question |
|---|---|
| ✈️ **Flight Performance** | Are flights operating on time, and where are delays happening? |
| 🗺️ **Network Efficiency** | Which routes are efficient and which are problematic? |
| 🏢 **Airport Operations** | Which airports are busiest or causing operational bottlenecks? |
| 🏷️ **Airline Performance** | Which airlines operate the most flights and which perform best? |
| 🔗 **Codeshare Complexity** | How do marketing airlines and operating airlines interact? |

---

## 🏗️ Architecture

```
AviationStack API
       │
       ▼
  Python Ingestion (scripts/)
       │
       ▼
┌─────────────────────────────────────────────────────┐
│                   dbt Pipeline                       │
│                                                      │
│  staging/       →  Raw API data cleaned & renamed    │
│  intermediate/  →  Spine joins, metrics, flags       │
│  core/          →  Shared dims, facts, codeshare     │
│  marts/         →  Analysis-ready aggregated tables  │
└─────────────────────────────────────────────────────┘
       │
       ▼
  Databricks (cloud warehouse)
       │
       ▼
  Streamlit (viz/)
  Interactive dashboard
```

### Layer Responsibilities

- **Staging** — 1:1 with source tables. Renames columns, casts types, applies light cleaning. No joins, no aggregations. Materialised as views.
- **Intermediate** — Three model types: `*_spine` (joins), `*_metrics` (derived flags and pre-classified columns), `*_features` (complex derivations). Business logic lives here, not in exposed models.
- **Core** — Shared, reusable dimensions and facts (`dim_*`, `fct_*`). SQL is SELECT...FROM...JOIN only — no business logic.
- **Marts** — Domain-oriented aggregated tables. Reference `int_*_metrics` models so no business logic is repeated across marts.

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| [AviationStack API](https://aviationstack.com/) | Data source — real-world flight data |
| [dbt Core](https://docs.getdbt.com/) | Data transformation framework |
| [Databricks](https://www.databricks.com/) | Cloud analytical data warehouse |
| [Streamlit](https://streamlit.io/) | Interactive data visualisation |
| Python | API ingestion scripts |
| dbt tests | Data quality validation |

---

## 📂 Project Structure

```
flight-analytics-dbt/
├── models/
│   ├── staging/          # Source-aligned models (views) + tests
│   ├── intermediate/     # Spine, metrics, and feature models + tests
│   ├── core/             # Shared dims and facts + tests
│   └── marts/            # Domain-aggregated marts + tests
├── viz/                  # Streamlit dashboard
│   ├── app.py            # Home page
│   ├── pages/            # One page per business domain
│   └── utils/            # Shared DB connection + theme utility
├── scripts/              # AviationStack API ingestion
├── seeds/                # Static reference data
├── snapshots/            # SCD type-2 tracking
├── macros/               # Reusable Jinja macros
├── tests/                # Custom singular tests
├── analyses/             # Ad-hoc exploratory SQL
├── dbt_project.yml
└── packages.yml
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- dbt Core with Databricks adapter: `pip install dbt-databricks`

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/efuatutuwaa/flight-analytics-dbt.git
cd flight-analytics-dbt

# 2. Install dbt packages
dbt deps

# 3. Create a .env file with your Databricks credentials
cp .env.example .env  # then fill in your values

# 4. Run the full pipeline
dbt run

# 5. Run data quality tests
dbt test

# 6. Generate and serve documentation
dbt docs generate && dbt docs serve

# 7. Run the Streamlit dashboard
cd viz && streamlit run app.py
```

---

## 🧪 Data Quality

Tests are defined at every layer of the pipeline. Coverage includes:

- **Uniqueness** — Primary key uniqueness enforced on all core and mart models
- **Not-null** — Critical fields validated across all layers
- **Accepted values** — Flight status and enumerations validated
- **Referential integrity** — Relationships between flights, routes, airports, and carriers tested
- **Conditional not-null** — Delay minutes validated as non-null for non-cancelled flights
- **Range checks** — Delay values validated within realistic bounds (-120 to 1440 mins)

```bash
dbt test
```

---

## 📊 Data Models

### Core

| Model | Grain | Description |
|---|---|---|
| `fct_flights` | 1 row per flight | Core flight fact — timing, delay, duration, and status |
| `dim_airline` | 1 row per airline | Airline attributes — name, type, fleet info |
| `dim_airport` | 1 row per airport | Airport attributes — location, timezone, geography |
| `dim_routes` | 1 row per route | Route geometry — distance, label, international flag |
| `fct_codeshare` | 1 row per flight × marketing airline | Operating vs. marketing airline pairings |

### Marts

| Mart | Grain | Description |
|---|---|---|
| `mart_flight_performance` | route + airline + date | Daily route-level performance per carrier |
| `mart_airline_performance` | 1 row per airline | Carrier-level volumes, on-time rate, reliability |
| `mart_route_performance` | 1 row per route | Route efficiency — delays, cancellations, duration |
| `mart_airport_operations` | 1 row per airport | Airport traffic volumes and performance |

---

## 📈 Visualisations

Interactive dashboard built with **Streamlit**, querying mart models directly from Databricks.

| Page | Business Question |
|---|---|
| Airline Performance | On-time rates, delay scatter, and cancellation rates by airline |
| Route Performance | Most delayed routes and route network treemap |
| Airport Operations | Busiest airports and on-time departure rates |

---

## 🔑 Key Concepts Demonstrated

- ✅ Spine/metrics/features intermediate pattern — business logic defined once, aggregated upward
- ✅ Multi-layer transformation architecture (staging → intermediate → core → marts)
- ✅ Exposed model purity — no business logic in `dim_*`, `fct_*`, or mart SQL
- ✅ Metrics defined once in `int_flight_metrics`, aggregated upward by all marts
- ✅ Custom schema per layer (`staging_models`, `intermediate_models`, `core_models`, `mart_models`)
- ✅ Kimball-style dimensional modelling (facts, dimensions, grain design)
- ✅ NULL-guarded timestamp calculations with timezone normalisation to UTC
- ✅ Data quality tests at every layer including range checks and conditional not-nulls
- ✅ dbt packages (`dbt_utils`)
- ✅ Seeds, snapshots, macros scaffolded

---

## 🔗 Project Origin

This project is the analytics engineering evolution of an earlier ETL pipeline built on the same dataset. The original project focused on raw data ingestion, staging, and storage using Python and AWS RDS (MySQL) — without a transformation layer.

👉 **[View the original ETL project → data-driven-sql](https://github.com/efuatutuwaa/data-driven-sql)**

That foundation informed the data modelling decisions made here, and this repo represents the next step: applying analytics engineering patterns — dbt, layered transformations, data quality, and dimensional modelling — on top of a properly ingested dataset.

---

## 👤 Author

**Efua Tutuwaa**
Analytics Engineering Capstone — 2026
[GitHub](https://github.com/efuatutuwaa)
