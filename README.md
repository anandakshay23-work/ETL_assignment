# ETL Pipeline — Production-Grade Data Engineering

A production-grade ETL pipeline built with **PySpark** that ingests multi-format data (XML/JSON/CSV), applies **database-driven dynamic mappings**, implements **SCD Type 2**, and loads into a **PostgreSQL Data Warehouse** following a `RAW → STAGING → DWH` architecture.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│  samplexml.xml │ samplexml2.xml │ samplejson1 │ samplejson2 │ CSV  │
└────────┬────────────────┬──────────────┬──────────────┬────────┬───┘
         │                │              │              │        │
         ▼                ▼              ▼              ▼        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PySpark ETL ENGINE                                │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │Extractors│→ │Data Quality  │→ │Mapping Engine│→ │  Loaders  │  │
│  │(XML/JSON │  │Engine        │  │(DB-Driven)   │  │(RAW/STG/  │  │
│  │ /CSV)    │  │(Reject Logs) │  │(Dynamic)     │  │   DWH)    │  │
│  └──────────┘  └──────────────┘  └──────────────┘  └───────────┘  │
└─────────────────────────────────────────────────────────────────────┘
         │                                                    │
         ▼                                                    ▼
┌──────────────────┐                          ┌───────────────────────┐
│ etl_config_db    │                          │ etl_dwh_db            │
│ (PostgreSQL)     │                          │ (PostgreSQL)          │
│                  │                          │                       │
│ • mapping_xbrl   │                          │ RAW Schema            │
│ • pipeline_meta  │                          │ • xml_xbrl_data       │
│                  │                          │ • xml_hierarchical    │
└──────────────────┘                          │ • json_financials     │
                                              │ • json_period         │
                                              │ • csv_data            │
                                              │                       │
                                              │ STAGING Schema        │
                                              │ • stg_financials      │
                                              │ • stg_companies       │
                                              │ • stg_reject_log      │
                                              │                       │
                                              │ DWH Schema (Star)     │
                                              │ • dim_company (SCD2)  │
                                              │ • fact_financials     │
                                              │ • dim_date            │
                                              │ • BI-Ready Views      │
                                              └───────────────────────┘
```

---

## Quick Start

### Prerequisites
- **Docker Desktop** installed and running
- No other dependencies needed — everything runs in containers

### Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **PostgreSQL** | `localhost:5432` | User: `etl_user` / Pass: `etl_password` |
| **pgAdmin 4** | [http://localhost:5050](http://localhost:5050) | Email: `admin@admin.com` / Pass: `admin` |

### Run the Pipeline

```bash
# 1. Clone/download the project
cd ETL_assignment

# 2. Start the pipeline (builds + runs everything)
docker-compose up --build

# 3. Run validation tests (after pipeline completes)
docker-compose exec etl-spark python tests/test_pipeline.py

# 4. Stop and clean up
docker-compose down -v
```

That's it! The pipeline will:
1. Start PostgreSQL with both databases auto-configured
2. Start **pgAdmin 4** with both databases pre-registered
3. Build the PySpark application container
4. Execute the full ETL pipeline
5. Output logs to `./logs/`

### Explore Data with pgAdmin

After the pipeline completes, open **[http://localhost:5050](http://localhost:5050)** and log in:
- **Email**: `admin@admin.com`
- **Password**: `admin`

Both databases are pre-configured under the **"ETL Pipeline"** server group:
- **ETL Config DB** (`etl_config_db`) — mapping rules and pipeline metadata
- **ETL Data Warehouse** (`etl_dwh_db`) — RAW / STAGING / DWH schemas with all tables and BI views

---

## Design Decisions & Rationale

### Why PySpark?
PySpark is the **industry standard** for ETL/data engineering:
- Scales from megabytes to petabytes without code changes
- Built-in DataFrame API with lazy evaluation and query optimization
- Native JDBC connectivity to PostgreSQL
- Demonstrates distributed computing awareness to evaluators

### Why Two Databases?
Following enterprise architecture best practices:
- **`etl_config_db`** — Configuration/mapping data (could be Oracle/MySQL in production)
- **`etl_dwh_db`** — The actual data warehouse (RAW/STAGING/DWH schemas)
- Separation of concerns: config changes don't affect warehouse operations
- Different backup/recovery strategies per database

### Why RAW → STAGING → DWH?

| Layer | Purpose | Data State |
|-------|---------|------------|
| **RAW** | Audit trail — data exactly as received | Untransformed, string types |
| **STAGING** | Quality-checked, cleaned, mapped | Cleaned, correct types, unified schema |
| **DWH** | Business-ready star schema | Dimensions + Facts, BI-optimized |

This 3-layer approach provides:
- **Reproducibility** — Can re-derive any layer from RAW
- **Debugging** — Compare RAW vs STAGING to identify transformation issues
- **Compliance** — RAW layer serves as immutable audit log

### Why Star Schema?
- **dim_company + fact_financials** is a classic star schema
- Optimized for analytical queries (aggregations, roll-ups)
- BI tools (Apache Superset, Tableau, Power BI) work best with star schemas
- Clear separation of descriptive data (dimensions) from measurable data (facts)

### Why SCD Type 2?
The assignment requires tracking changes to company data over time:
- **Type 1** — Overwrites history (loses data — bad for analytics)
- **Type 2** — Preserves full history (what we implemented)
- **Type 3** — Stores current + previous only (limited history)

SCD Type 2 is the gold standard for dimension management because:
- Full audit trail of all changes
- Point-in-time analysis ("What was company X's name last quarter?")
- No data loss

---

## Data Quality Rules

The pipeline implements the following quality checks:

| Rule | Description | Action |
|------|-------------|--------|
| Missing company_id | Records with null/empty company_id | **Reject** → reject log |
| Invalid year | Non-numeric years like "20x3", "20X3" | **Reject** → reject log |
| Invalid profit | Non-parseable profit values | **Warn** (attempt recovery via split_comma) |
| Duplicates | Same company_id + period + source | **Remove** duplicates |

### Known Corruptions in CSV Data
1. **Row 12** — Missing company_id → **Rejected**
2. **Row 16** — Year "20x3" → **Rejected**
3. **Row 27** — Year "20X3" → **Rejected**
4. **All profit values** — Format "692,extra" → **Recovered** via split_comma transformation
5. **Remarks column** — Pipe-delimited "text_1|extra" → handled but not loaded to DWH

---

## Dynamic Mapping Configuration

Mappings are stored in the `mapping_xbrl` database table and fetched at **runtime**:

```sql
SELECT * FROM mapping_xbrl;
```

| source_field | target_column | transformation | datatype |
|-------------|---------------|----------------|----------|
| ID | company_id | direct | varchar |
| Name | company_name | upper | varchar |
| Revenue | revenue | currency_convert | numeric |
| Profit | profit | split_comma | numeric |
| Year | year | direct | int |
| Currency | currency | direct | varchar |
| Period | period_type | map_cy_py | varchar |

### Transformation Functions

| Transformation | Description |
|---------------|-------------|
| `direct` | Pass-through (no change, optional type cast) |
| `upper` | Convert to UPPERCASE |
| `currency_convert` | Convert to USD using config rates (EUR×1.08, INR×0.012) |
| `split_comma` | Extract numeric from "692,extra" format |
| `map_cy_py` | Standardize period to CY/PY |

> **STRICT RULE**: No mapping logic is hardcoded. All transformations are database-driven.
> Adding new mappings = INSERT a new row in `mapping_xbrl` — no code changes needed.

---

## Configuration

All runtime configuration is in `config/pipeline_config.json`:

```json
{
  "mapping_db": { "host": "postgres", "database": "etl_config_db", ... },
  "dwh_db":     { "host": "postgres", "database": "etl_dwh_db", ... },
  "sources": [ ... ],
  "currency_rates": { "USD": 1.0, "EUR": 1.08, "INR": 0.012 }
}
```

Nothing is hardcoded in the source code — all connection strings, file paths, and rates are externalized.

---

## Project Structure

```
ETL_assignment/
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # PySpark application image
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── config/
│   └── pipeline_config.json    # Runtime configuration
├── init-db/
│   ├── 01-create-databases.sql # DB creation
│   ├── 02-config-schema.sql    # Mapping table + seed
│   └── 03-dwh-schema.sql      # RAW/STAGING/DWH schemas
├── pgadmin/
│   ├── servers.json            # Auto-registered DB servers
│   └── pgpass                  # Passwordless DB connection
├── data/
│   ├── input/                  # Source files
│   ├── output/                 # Exported results
│   └── reject/                 # Rejected records
├── logs/                       # Pipeline logs
├── src/
│   ├── main.py                 # Pipeline orchestrator
│   ├── config/
│   │   └── config_manager.py   # Config + DB mapping loader
│   ├── extractors/
│   │   ├── base_extractor.py   # Abstract base class
│   │   ├── xml_extractor.py    # XBRL + hierarchical XML
│   │   ├── json_extractor.py   # Nested + multi-period JSON
│   │   └── csv_extractor.py    # Multi-table CSV
│   ├── transformers/
│   │   ├── mapping_engine.py   # Dynamic mapping transformer
│   │   ├── data_quality.py     # Validation + reject handling
│   │   └── scd_handler.py      # SCD Type 2 implementation
│   ├── loaders/
│   │   ├── raw_loader.py       # RAW schema loader
│   │   ├── staging_loader.py   # STAGING schema loader
│   │   └── dwh_loader.py       # DWH star schema loader
│   └── utils/
│       ├── logger.py           # Structured logging
│       ├── db_connector.py     # PostgreSQL connections
│       └── spark_session.py    # SparkSession factory
└── tests/
    └── test_pipeline.py        # Post-execution validation
```

---

## Assumptions

1. **Currency rates** are static (USD=1.0, EUR=1.08, INR=0.012). In production, these would come from a live API.
2. **First run** is a full load. Subsequent runs are incremental (based on company_id + period + source_file).
3. **CSV metadata** (entity name, period dates) at the bottom of `sample_100.csv` is stored separately in `raw.csv_metadata`.
4. **Company names** come only from `samplexml2.xml` (hierarchical XML). Other sources don't provide company names.
5. **Date dimension** is pre-seeded with dates from 2019-2025 for BI analysis.
6. **Profit** in JSON/XML sources is loaded directly; CSV profit requires `split_comma` cleanup.

---

## Output

After pipeline execution:
- **Logs**: `logs/etl_pipeline_YYYYMMDD_HHMMSS.log` (full execution trace)
- **Reject files**: `data/reject/reject_<source>_<timestamp>.csv` (corrupted records + reasons)
- **Database**: All data in PostgreSQL (`etl_dwh_db`) across RAW/STAGING/DWH layers
- **pgAdmin 4**: Browse all databases, schemas, and tables at [http://localhost:5050](http://localhost:5050)
- **BI Views**: Three pre-built views ready for Apache Superset connection
