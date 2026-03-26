# ✈️ Flight Analytics — dbt Capstone Project

> An end-to-end analytics engineering project that models real-world flight data from the **AviationStack API** using **dbt** and **Databricks**. Built to demonstrate production-grade data modeling, transformation layering, semantic layer design, and data quality practices.

---

## 📌 Project Overview

This project ingests live flight data from the AviationStack API and transforms it through a structured multi-layer dbt pipeline into analysis-ready data marts. It answers five core business questions about the global aviation network:

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
│  staging/       →  Raw API data cleaned & typed      │
│  intermediate/  →  Business logic & joins            │
│  core/          →  Reusable shared entities          │
│  marts/         →  Analysis-ready fact/dim tables    │
│  semantic/      →  MetricFlow semantic models        │
└─────────────────────────────────────────────────────┘
       │
       ▼
  Databricks (cloud warehouse)
       │
       ├──────────────────────┐
       ▼                      ▼
  LookML (looker/)       Streamlit (viz/)
  Semantic layer         Interactive dashboard
```

### Layer Responsibilities

- **Staging** — 1:1 with source tables. Renames columns, casts types, and applies light cleaning. No joins.
- **Intermediate** — Applies business logic, resolves relationships, and prepares enriched entities for downstream use.
- **Core** — Shared, reusable building blocks (e.g. resolved flight entities, carrier dimensions) referenced across multiple marts.
- **Marts** — Domain-oriented, analysis-ready tables. Each mart maps to one of the five business domains above.
- **Semantic** — MetricFlow semantic models and metric definitions built on top of core models (in progress).

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| [AviationStack API](https://aviationstack.com/) | Data source — real-time & historical flight data |
| [dbt Core](https://docs.getdbt.com/) | Data transformation framework |
| [Databricks](https://www.databricks.com/) | Cloud analytical data warehouse |
| [LookML](https://cloud.google.com/looker/docs/what-is-lookml) | Semantic layer for Looker BI |
| [MetricFlow](https://docs.getdbt.com/docs/build/about-metricflow) | dbt semantic layer (in progress) |
| [Streamlit](https://streamlit.io/) | Interactive data visualisation |
| Python | API ingestion scripts |
| dbt tests | Data quality validation |

---

## 📂 Project Structure

```
flight-analytics-dbt/
├── models/
│   ├── staging/          # Source-aligned models + tests
│   ├── intermediate/     # Business logic layer + tests
│   ├── core/             # Shared entities + tests
│   ├── marts/            # Domain marts + tests
│   └── semantic/         # MetricFlow semantic models (in progress)
├── looker/               # LookML semantic layer
│   ├── flight_analytics.model.lkml
│   └── views/            # One view per mart
├── viz/                  # Streamlit dashboard
│   ├── app.py            # Home page
│   ├── pages/            # One page per business domain
│   └── utils/            # Shared DB connection utility
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

Tests are defined at every layer of the pipeline — staging, intermediate, and marts. Coverage includes:

- **Uniqueness** — Primary key uniqueness enforced on all mart and core models
- **Not-null** — Critical fields validated across all layers
- **Accepted values** — Flight status, delay categories, and other enumerations validated
- **Referential integrity** — Relationships between flights, routes, airports, and carriers tested
- **Conditional not-null** — Delay minutes validated as non-null for non-cancelled flights
- **Range checks** — Delay values validated within realistic bounds (-120 to 1440 mins)

Run all tests with:
```bash
dbt test
```

---

## 📊 Data Marts

| Mart | Grain | Description |
|---|---|---|
| `fct_flights` | 1 row per flight | Core flight fact — timing, delay, and status metrics |
| `mart_airline_performance` | 1 row per airline | Carrier-level flight volumes, on-time performance, and reliability |
| `mart_route_performance` | 1 row per route | Route efficiency — delays, cancellations, duration, and competition |
| `mart_airport_operations` | 1 row per airport | Airport traffic volumes and departure/arrival performance |
| `fct_codeshare` | 1 row per flight × marketing airline | Marketing vs. operating airline codeshare relationships |

---

## 🔍 Semantic Layer

### LookML (`looker/`)
Looker semantic layer built on top of the dbt marts. Defines dimensions, measures, and explores for self-serve analytics in Looker.

- `mart_airline_performance.view.lkml` — Airline dimensions and performance measures
- `mart_route_performance.view.lkml` — Route dimensions and efficiency measures
- `mart_airport_operations.view.lkml` — Airport dimensions and traffic measures
- `flight_analytics.model.lkml` — Explores for each business domain

### MetricFlow (`models/semantic/`)
dbt native semantic layer using MetricFlow. Defines reusable metric definitions on top of core fact tables. *(In progress)*

---

## 🔑 Key dbt Concepts Demonstrated

- ✅ Multi-layer transformation architecture (staging → intermediate → core → marts)
- ✅ Custom schema per layer (`staging_models`, `intermediate_models`, `core_models`, `mart_models`)
- ✅ Kimball-style dimensional modeling (facts, dimensions, grain design)
- ✅ Data quality guards at the intermediate layer (null timestamp protection)
- ✅ LookML semantic layer on top of dbt marts
- ✅ dbt Semantic Layer with MetricFlow (in progress)
- ✅ Snapshots for slowly changing dimension (SCD) tracking
- ✅ Reusable Jinja macros
- ✅ dbt packages (via `packages.yml`)
- ✅ Data quality tests at every layer
- ✅ Seeds for static reference data

---

## 📈 Visualizations

Interactive dashboard built with **Streamlit**, querying mart models directly from Databricks.

| Page | Business Question |
|---|---|
| Airline Performance | On-time rates, delay scatter, and cancellation rates by airline |
| Route Performance | Most delayed routes treemap and route network analysis |
| Airport Operations | Busiest airports and on-time departure rates by airport |

---

## 🔗 Project Origin

This project is the analytics engineering evolution of an earlier ETL pipeline built on the same dataset. The original project focused on raw data ingestion, staging, and storage using Python and AWS RDS (MySQL) — without a transformation layer.

👉 **[View the original ETL project → data-driven-sql](https://github.com/efuatutuwaa/data-driven-sql)**

That foundation informed the data modeling decisions made here, and this repo represents the next step: applying analytics engineering patterns — dbt, layered transformations, data quality, and a semantic layer — on top of a properly ingested dataset.

---

## 👤 Author

**Efua Tutuwaa**
Analytics Engineering Capstone — 2026
[GitHub](https://github.com/efuatutuwaa)
