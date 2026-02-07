# Weather Data Ingestion Pipeline

## TL;DR

### Quick Start

**Local Execution**
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
$env:OPENWEATHER_API_KEY="your_api_key_here"
pip install -r requirements.txt
python -m src.main
```

**Docker Execution**
```bash
docker build -t weather-pipeline .
docker run -e OPENWEATHER_API_KEY=your_api_key_here
```

### Project Structure
```
weather_pipeline/
├── config/          # Pipeline inputs & ingestion config
├── data/            # Output datasets (partitioned)
├── src/             # Pipeline implementation
├── Dockerfile       # Containerized execution
├── requirements.txt
└── README.md
```

### Config Highlights

- `config/config.yaml` controls pipeline behavior
- Cities validated against `config/city.list.json`
- Storage formats: CSV / JSON / Parquet
- Partition layouts: date / date_country / country_date / hive_compact / city_date

---

## 1. Project Overview

This project implements a configurable **batch data pipeline** that collects current weather data from the **OpenWeather API** and stores it as structured datasets suitable for analytics and data science experimentation.

**Key characteristics:**

- Batch ingestion (on-demand)
- Config-driven scope (cities, storage strategy, units)
- Validated schema using a typed model layer
- Multiple storage formats (CSV, JSON, Parquet)
- Partitioned datasets for efficient querying
- Deterministic and idempotent runs (timestamped batches)

**Typical usage:**
Collect operational external data → store in data lake → enable analytics & modeling

---

## 2. Architecture

### Pipeline Flow
```
Configuration
     ↓
City Validation
     ↓
API Collection (with retries)
     ↓
Normalization & Semantic Modeling
     ↓
Partitioned Storage
```

### Responsibilities by Layer

| Layer         | Responsibility                                  |
|---------------|-------------------------------------------------|
| Config        | Defines ingestion scope and behavior            |
| API Client    | External communication and resilience           |
| Model Layer   | Schema validation & semantic normalization      |
| Storage Layer | Dataset materialization optimized for analytics |

**Design principle:** Separation of ingestion, transformation, and persistence ensures **extensibility and testability**.

---

## 3. Project Structure

```
weather_pipeline/
├── config/
│   ├── city.list.json
│   └── config.yaml
├── data/
├── src/
│   ├── api_client.py
│   ├── config_loader.py
│   ├── logger.py
│   ├── main.py
│   ├── models.py
│   └── storage.py
├── Dockerfile
├── requirements.txt
└── README.md
```

### Folder Responsibilities

| Folder       | Purpose                            |
|--------------|------------------------------------|
| config       | Pipeline inputs & ingestion config |
| data         | Output datasets (partitioned)      |
| src          | Pipeline implementation            |
| Dockerfile   | Containerized execution            |

---

## 4. Configuration

Pipeline behavior is fully controlled via `config/config.yaml`.

### 4.1 API Configuration

Defines how the pipeline communicates with OpenWeather.

**Parameters:**

- API endpoint
- Unit system (metric / imperial / default)
- Retry attempts and delay strategy

> Retry mechanism handles transient network failures and temporary API unavailability.

### 4.2 Storage Configuration

| Parameter  | Description                             |
|------------|-----------------------------------------|
| format     | Output file format (csv, json, parquet) |
| layout     | Partition structure                     |
| base_path  | Destination dataset location            |

### 4.3 Cities Input

Cities are defined in the YAML file and validated against `city.list.json`.
Invalid cities are filtered before any API request to prevent unnecessary external calls.

---

## 5. Data Model & Transformations

Raw API responses are normalized through a typed model (`WeatherRecord`).

| Field          | Transformation                         |
|----------------|----------------------------------------|
| Timestamp      | Standardized datetime format           |
| Timezone       | Converted to UTC±offset string         |
| Wind direction | Degrees → compass direction (16-point) |
| Units          | Added to column headers                |
| Missing values | Explicitly set to `NULL`               |

**Goal:** Convert API-shaped data into **analytics-ready structured data**.

---

## 6. Storage Strategy

Supports multiple dataset formats and partition layouts for different analytical workloads.

### 6.1 Supported Formats

| Format | Best Use              | Tradeoff                       |
|--------|-----------------------|--------------------------------|
| CSV    | Human inspection      | No schema, inefficient queries |
| JSON   | Interoperability      | Large footprint                |
| Parquet| Analytics & data lakes| Not human-readable             |

> **Recommendation:** Use Parquet for analytical environments due to column pruning and predicate pushdown.

### 6.2 Partition Layouts

| Layout       | Structure                   | Optimized For       | Tradeoff                 |
|--------------|-----------------------------|---------------------|--------------------------|
| date         | year/month/day              | Time filtering      | Harder per-city queries  |
| date_country | year/month/day/country      | Country analysis    | File explosion           |
| country_date | country/year/month/day      | Country aggregation | Potential skew           |
| hive_compact | year/month/day/country/city | General analytics   | Many small dirs          |
| city_date    | city/year/month/day         | Per-city ML         | Expensive global queries |

> All files generated in the same execution share a **run timestamp**, ensuring batch consistency.

---

## 7. Pipeline Execution

### Local Execution
```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
$env:OPENWEATHER_API_KEY="your_api_key_here"
pip install -r requirements.txt
python -m src.main
```

### Docker Execution
```bash
docker build -t weather-pipeline .
docker run -e OPENWEATHER_API_KEY=your_api_key_here
```

---

## 8. Output Dataset

Each execution produces a **new batch** identified by a run timestamp, example:
```
weather_20260207_190026.parquet
```

**Characteristics:**

- Immutable batch outputs
- Partitioned directories
- Stable schema
- Null-safe fields
- Analytics-ready layout

---

## 9. Reliability & Operational Behavior

| Feature            | Behavior                         |
|------------------- |----------------------------------|
| Retry mechanism    | Handles transient API failures   |
| City validation    | Prevents invalid API calls       |
| Structured logging | Execution traceability           |
| Timestamped batches| Idempotent executions            |

> Multiple runs per day are supported without overwriting previous data.

---

## 10. Improvements

**Data Scope:** Forecast ingestion, Historical weather collection
**Automation:** Scheduler / orchestrator integration (Airflow, cron)
**Observability:** Metrics, Persistent log storage
**Data Quality:** Completeness checks, Schema validation rules

---

## 11. Assumptions & Limitations

- Designed as a **batch ingestion component** (not streaming)
- Container is **lightweight** for development usage
- API rate limits depend on **OpenWeather subscription tier**
- City dataset must match **OpenWeather schema**

---

## 12. Design Decisions & Personal Notes

- **Dockerization:** Since I had no prior experience with Docker, the container is included for completeness, but may require further testing or team review in a real scenario. Focus was prioritized on building a structured and self-contained Python pipeline.

- **Development Environment:** Chose VS Code for efficiency and flexibility.

- **Data Source & Validation:**
  - `city.list.json`, from OpenWeather (https://bulk.openweathermap.org/sample/city.list.json.gz), is used to validate user-provided cities before making API calls. This ensures invalid cities are filtered early, preventing unnecessary API requests. The list includes 209579 cities across the globe.
  - Configurable city list allows easy addition/removal of cities for different use cases.

- **Configuration Decisions:**
  - `.yaml` format chosen for `config.yaml` due to readability and human-friendly structure.
  - API section includes retries to handle transient network/API failures.
  - Storage section is fully configurable with `format`, `layout`, and `base_path`.
  - Encoding of UTF-8 to guarantee handling of special characters in cities names (eg. "Köln", in Germany).

- **Storage Format Choice:**
  - **Parquet** selected as default for analytical workloads (supports column pruning, Hive partitions, fast queries with Spark, Pandas, BigQuery).
  - CSV and JSON remain available for human inspection or interoperability, but less optimal for analytics.

- **Partition Layout Choice:**
  - **hive_compact** chosen as default: balances time, country, and city partitions, maximizing predicate pushdown and supporting multiple analysis patterns.
  - Other layouts (`date`, `date_country`, `country_date`, `city_date`) remain configurable for different analytical needs.
  - Decisions based on exercise goal: provide data for Data Scientists to test hypotheses efficiently.

- **Weather Data Modeling:**
  - Transformations in `models.py` improve interpretability (e.g., wind direction converted from degrees to compass points).
  - Column names reflect units (metric or imperial or default).
  - Missing or failed fields forced to `None` to guarantee consistent schema.

- **Pipeline Robustness:**
  - Idempotent runs with timestamped output files ensure multiple executions per day without overwriting data.
  - Logging implemented via `logger.py` (stdout for now, but extensible to log tables or files).

- **Future Improvements / Trade-offs:**
  - Add forecast collection for multi-day data.
  - Include automated scheduling or orchestrator integration.
  - Enhance logging, validation, and data quality checks (nulls, datatypes, completeness).
  - Dynamic city discovery (instead of manual list) to reduce human intervention.
  - Consider alternative handling of missing fields instead of forcing `None`.
