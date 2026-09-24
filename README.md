# Weather Data Engineering Pipeline

An end-to-end batch data pipeline that ingests weather forecast data from the Open-Meteo API, processes it through a medallion architecture (Bronze → Silver → Gold), and delivers business-ready analytics tables — all orchestrated on Databricks with serverless compute.

---

## Architecture Overview

This project follows the **medallion architecture** pattern, a data engineering best practice that organizes data into progressively refined layers:

| Layer | Purpose | Data Shape | Key Transformations |
| --- | --- | --- | --- |
| **Ingestion** | Fetch raw data from Open-Meteo API | JSON files in UC Volumes | API calls, partitioned storage, audit logging |
| **Bronze** | Land raw data into Delta tables | Grows by 1 row per run | Schema enforcement, technical columns (lineage) |
| **Silver** | Clean, flatten, and validate | Grows by 168 rows per run | `explode()`, `arrays_zip()`, data quality flags |
| **Gold** | Business metrics and aggregations | Grows by 7 rows per run | `groupBy().agg()`, window functions, business logic |

### Data Flow

```
Open-Meteo API
      |
      v
[Ingestion Notebook]
      |  Fetches 7-day forecast for Austin, TX
      |  Saves partitioned JSON to Unity Catalog Volumes
      v
[Bronze Layer]
      |  Reads JSON with PySpark (lazy evaluation)
      |  Adds lineage columns (load_timestamp, source_file)
      |  Writes to Delta table with ACID guarantees
      v
[Silver Layer]
      |  Explodes nested arrays into flat rows (168 hourly observations)
      |  Converts string timestamps to TIMESTAMP type
      |  Adds data quality validation flags
      |  Writes hourly + daily Delta tables
      v
[Gold Layer]
      |  Aggregates hourly → daily summaries (avg, min, max, sum)
      |  Adds window functions (lag, lead, row_number) for trends
      |  Rolls up daily → weekly summaries
      |  Adds business flags (is_hot_day, is_rainy_day)
      |  Writes Gold Delta tables for BI consumption
```

---

## Tech Stack

| Technology | Role |
| --- | --- |
| **Databricks** | Cloud platform for data engineering and analytics |
| **PySpark** | Distributed data processing framework |
| **Delta Lake** | ACID transactional storage format with time travel |
| **Unity Catalog** | Data governance, access control, and lineage |
| **Databricks Jobs** | Pipeline orchestration with serverless compute |
| **Open-Meteo API** | Free weather forecast data source |

---

## Key PySpark Concepts Demonstrated

### Array Transformations

- **`arrays_zip()`** — Combines parallel arrays (time[], temp[], humidity[]) into a single array of structs, keeping related values aligned
- **`explode()`** — Converts array elements into individual rows (1 row with 168-element array → 168 flat rows)

### Aggregations

- **`groupBy().agg()`** — Groups hourly observations by date and computes summary statistics (avg, min, max, sum, count)
- **Multi-level rollups** — Hierarchical aggregation from hourly → daily → weekly

### Window Functions

- **`lag()`** — Retrieves the previous day's average temperature for day-over-day comparison
- **`lead()`** — Retrieves the next day's forecast for trend analysis
- **`row_number()`** — Assigns sequential numbers to days in the forecast

### Data Quality

- **`when().otherwise()`** — Conditional logic to flag invalid temperatures (outside -50°F to 130°F) and humidity (outside 0-100%)
- **Boolean flags** — `is_valid_temperature`, `is_valid_humidity` columns allow filtering without data loss

### Delta Lake Features

- **ACID transactions** — No partial writes if the pipeline fails mid-execution
- **Time travel** — Query table state at any previous version (`SELECT * FROM table VERSION AS OF 1`)
- **Schema enforcement** — `overwriteSchema` option ensures structural integrity

---

## Pipeline Orchestration

The pipeline is orchestrated using **Databricks Jobs** with serverless compute:

```
[Ingestion] -> [Bronze_Layer] -> [Silver_Layer] -> [Gold_Layer]
   Task 1        Task 2            Task 3          Task 4
```

| Setting | Value |
| --- | --- |
| Compute | Serverless (auto-scaling, pay-per-second) |
| Schedule | Daily at 6:00 AM UTC |
| Task Dependencies | Sequential (each task depends on previous) |
| Max Concurrent Runs | 1 |
| Failure Handling | Downstream tasks skip on failure |

### Incremental Loading

All layers use `mode("append")` to accumulate data over time. Each daily run adds a new batch of forecast data:

```
Day 1:  Bronze = 1 row,   Silver = 168 rows,   Gold = 7 rows
Day 2:  Bronze = 2 rows,  Silver = 336 rows,   Gold = 14 rows
Day 30: Bronze = 30 rows, Silver = 5,040 rows, Gold = 210 rows
```

This enables historical trend analysis — you can compare how forecasts evolved over time and measure prediction accuracy.

### Why Serverless?

Serverless compute eliminates cluster startup overhead (seconds vs minutes), auto-scales to workload size, and bills only for actual compute time — ideal for a lightweight batch pipeline with 4 short tasks.

---

## Project Structure

```
weather-data-pipeline/
|-- Ingestion          # Fetches weather data from Open-Meteo API
|-- Bronze_Layer       # Loads raw JSON into Bronze Delta table
|-- Silver_Layer       # Explodes arrays, validates data, writes Silver tables
|-- Gold_Layer         # Creates business metrics, aggregations, window functions
`-- README.md          # This file
```

---

## Unity Catalog Tables

| Layer | Table Name | Description |
| --- | --- | --- |
| Bronze | `my_project_catalog.weather_schema.bronze_weather_raw` | Raw nested API response |
| Silver | `my_project_catalog.weather_schema.silver_weather_hourly` | 168 hourly observations (flat) |
| Silver | `my_project_catalog.weather_schema.silver_weather_daily` | 7 daily forecast summaries |
| Gold | `my_project_catalog.weather_schema.gold_daily_weather_summary` | Daily metrics + business flags |
| Gold | `my_project_catalog.weather_schema.gold_weekly_weather_summary` | Weekly rollups |

---

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Serverless compute configured
- Open-Meteo API access (free, no API key required)

### Setup

1. Create a Unity Catalog catalog and schema:
   ```sql
   CREATE CATALOG IF NOT EXISTS my_project_catalog;
   CREATE SCHEMA IF NOT EXISTS my_project_catalog.weather_schema;
   CREATE VOLUME IF NOT EXISTS my_project_catalog.weather_schema.landing_stage;
   ```

2. Run notebooks in order:
   - `Ingestion` → fetches weather data
   - `Bronze_Layer` → loads into Delta table
   - `Silver_Layer` → cleans and flattens data
   - `Gold_Layer` → creates business metrics

3. Or set up the Databricks Job to run all 4 notebooks automatically on a schedule.

---

## Sample Queries

```sql
-- Top 3 hottest days
SELECT observation_date, max_temperature, temperature_category, is_hot_day
FROM my_project_catalog.weather_schema.gold_daily_weather_summary
ORDER BY max_temperature DESC
LIMIT 3;

-- Days with significant temperature swings
SELECT observation_date, temperature_range, temp_change_from_prev_day
FROM my_project_catalog.weather_schema.gold_daily_weather_summary
WHERE temp_change_from_prev_day IS NOT NULL
ORDER BY ABS(temp_change_from_prev_day) DESC;

-- Weekly weather summary
SELECT week_start_date, weekly_avg_temperature, weekly_total_precipitation
FROM my_project_catalog.weather_schema.gold_weekly_weather_summary
ORDER BY week_start_date;
```

---

## Future Enhancements

- [ ] Add more cities for multi-location comparison
- [x] Incremental loading with append mode (accumulates forecast history daily)
- [ ] Build a Lakeview dashboard on Gold tables
- [ ] Set up SQL alerts for extreme weather events
- [ ] Add data quality monitoring with expectations

---

## License

This project is open source and available under the MIT License.