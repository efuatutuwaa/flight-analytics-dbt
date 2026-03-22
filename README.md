# ✈️ Flight Analytics — dbt Capstone Project

> An end-to-end analytics engineering project that models real-world flight data from the **AviationStack API** using **dbt** and **Databricks**. Built to demonstrate production-grade data modeling, transformation layering, and data quality practices.

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
│  semantic/      →  Metric definitions (ephemeral)    │
└─────────────────────────────────────────────────────┘
       │
       ▼
  Databricks (cloud warehouse)
       │
       ▼
  Visualizations (viz/)
```

### Layer Responsibilities

- **Staging** — 1:1 with source tables. Renames columns, casts types, and applies light cleaning. No joins.
- **Intermediate** — Applies business logic, resolves relationships, and prepares enriched entities for downstream use.
- **Core** — Shared, reusable building blocks (e.g. resolved flight entities, carrier dimensions) referenced across multiple marts.
- **Marts** — Domain-oriented, analysis-ready tables. Each mart maps to one of the five business domains above.
- **Semantic** — Ephemeral metric definitions built on top of marts for consistent KPI reporting.

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| [AviationStack API](https://aviationstack.com/) | Data source — real-time & historical flight data |
| [dbt Core](https://docs.getdbt.com/) | Data transformation framework |
| [Databricks](https://www.databricks.com/) | Cloud analytical data warehouse |
| Python | API ingestion scripts |
| dbt tests | Data quality validation |

---

## 📁 Project Structure

```
flight-analytics-dbt/
├── models/
│   ├── staging/          # Source-aligned models + tests
│   ├── intermediate/     # Business logic layer + tests
│   ├── core/             # Shared entities + tests
│   ├── marts/            # Domain marts + tests
│   └── semantic/         # Metric definitions (ephemeral)
├── scripts/              # AviationStack API ingestion
├── seeds/                # Static reference data (airports, carriers)
├── snapshots/            # SCD type-2 tracking
├── macros/               # Reusable Jinja macros
├── tests/                # Custom singular tests
├── analyses/             # Ad-hoc exploratory SQL
├── viz/                  # Dashboard / visualization layer (WIP)
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

# 3. Ingest data from AviationStack API
#    (requires a free API key at https://aviationstack.com/)
python scripts/ingest.py --api-key YOUR_API_KEY

# 4. Run the full pipeline
dbt run

# 5. Run data quality tests
dbt test

# 6. Generate and serve documentation
dbt docs generate && dbt docs serve
```

---

## 🧪 Data Quality

Tests are defined at every layer of the pipeline — staging, intermediate, and marts. Coverage includes:

- **Uniqueness** — Primary key uniqueness enforced on all mart and core models
- **Not-null** — Critical fields validated across all layers
- **Accepted values** — Flight status, delay categories, and other enumerations validated
- **Referential integrity** — Relationships between flights, routes, airports, and carriers tested

Run all tests with:
```bash
dbt test
```

---

## 📊 Data Marts

| Mart | Description |
|---|---|
| `mart_flight_performance` | On-time rates, delay distributions, and delay root causes by flight |
| `mart_network_efficiency` | Route-level efficiency metrics — avg delays, cancellation rates, utilisation |
| `mart_airport_operations` | Airport traffic volume, departure/arrival performance, bottleneck indicators |
| `mart_airline_performance` | Carrier-level flight volumes, on-time performance, and operational reliability |
| `mart_codeshare_complexity` | Marketing vs. operating airline relationships and codeshare network analysis |

---

## 📐 Key dbt Concepts Demonstrated

- ✅ Multi-layer transformation architecture (staging → intermediate → core → marts)
- ✅ Custom schema per layer (`staging_models`, `intermediate_models`, `core_models`, `mart_models`)
- ✅ dbt Semantic Layer with ephemeral metric definitions
- ✅ Snapshots for slowly changing dimension (SCD) tracking
- ✅ Reusable Jinja macros
- ✅ dbt packages (via `packages.yml`)
- ✅ Data quality tests at every layer
- ✅ Seeds for static reference data

---

## 📈 Visualizations

Dashboard layer in progress — built on top of the mart models using **Streamlit**, querying directly from Databricks.

Planned views:
- Flight delay heatmap by airport and airline
- Route efficiency scorecard
- Airline performance comparison
- Codeshare network explorer

> Screenshots coming soon.

---

## 🤝 Author

**Efua Tutuwaa**
Analytics Engineering Capstone — 2026
[GitHub](https://github.com/efuatutuwaa)
