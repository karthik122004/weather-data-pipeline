# Databricks notebook source
# DBTITLE 1,Configuration
# Configuration - single source of truth for all settings

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
LOCATION_NAME = "Austin_TX"
LATITUDE = 30.2672
LONGITUDE = -97.7431

# Unity Catalog Volume Paths (landing zone for raw data)
VOLUME_BASE_PATH = "/Volumes/my_project_catalog/weather_schema/landing_stage"
DATA_FOLDER = f"{VOLUME_BASE_PATH}/weather"
LOG_FOLDER = f"{VOLUME_BASE_PATH}/logs"

# API Parameters - weather data to fetch
API_PARAMS = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
    "hourly": "temperature_2m,relative_humidity_2m,precipitation_probability,precipitation,weather_code",
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code",
    "temperature_unit": "fahrenheit",
    "wind_speed_unit": "mph",
    "precipitation_unit": "inch",
    "timezone": "America/Chicago",
    "forecast_days": 7
}

# COMMAND ----------

# DBTITLE 1,Helper Functions
from datetime import datetime
import os

def get_current_timestamp():
    """Generate timestamp for file naming."""
    return datetime.now()

def generate_partition_path(base_path, timestamp):
    """Create Hive-style partitioned path: year=YYYY/month=MM/day=DD"""
    year = timestamp.year
    month = f"{timestamp.month:02d}"
    day = f"{timestamp.day:02d}"
    return f"{base_path}/year={year}/month={month}/day={day}"

def generate_filename(location, timestamp):
    """Generate idempotent filename with timestamp."""
    timestamp_str = timestamp.strftime("%Y%m%d")
    return f"weather_{location}_{timestamp_str}.json"

def ensure_directory_exists(path):
    """Create folder if it doesn't exist (defensive programming)."""
    os.makedirs(path, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,API Fetch with Error Handling
import requests
import json
from typing import Tuple, Dict, Optional

def fetch_weather_data(api_url: str, params: dict) -> Tuple[bool, Optional[Dict], Optional[str]]:
    """Fetch weather data from API with error handling.
    Returns (success, data, error_message)."""
    try:
        # Request with 30s timeout to prevent hanging
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()  # Raises exception if HTTP status >= 400
        data = response.json()

        # Basic validation
        if not data or 'current' not in data:
            return False, None, "API returned empty or invalid data structure"

        return True, data, None

    except requests.exceptions.Timeout:
        return False, None, "API request timed out after 30 seconds"
    except requests.exceptions.ConnectionError:
        return False, None, "Network connection failed"
    except requests.exceptions.HTTPError as e:
        return False, None, f"HTTP error: {e.response.status_code} - {e.response.reason}"
    except json.JSONDecodeError:
        return False, None, "API returned invalid JSON"
    except Exception as e:
        return False, None, f"Unexpected error: {str(e)}"

# COMMAND ----------

# DBTITLE 1,Save Data with Metadata
def save_weather_data(data: dict, base_path: str, location: str,
                      timestamp: datetime) -> Tuple[bool, Optional[str], Optional[str]]:
    """Save weather data to partitioned folder with ingestion metadata.
    Returns (success, file_path, error_message)."""
    try:
        partition_path = generate_partition_path(base_path, timestamp)
        filename = generate_filename(location, timestamp)
        full_path = f"{partition_path}/{filename}"

        ensure_directory_exists(partition_path)

        # Enrich raw data with ingestion metadata for traceability
        enriched_data = {
            "metadata": {
                "ingestion_timestamp": timestamp.isoformat(),
                "source_api": API_BASE_URL,
                "location": location,
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "ingestion_version": "1.0"
            },
            "raw_data": data  # Original API response untouched
        }

        with open(full_path, 'w') as f:
            json.dump(enriched_data, f, indent=2)

        return True, full_path, None

    except Exception as e:
        return False, None, f"Error saving data: {str(e)}"

# COMMAND ----------

# DBTITLE 1,Logging & Audit Trail
import json

def write_ingestion_log(timestamp: datetime, success: bool,
                       file_path: Optional[str] = None,
                       error_message: Optional[str] = None):
    """Write append-only ingestion log entry (JSONL format, one log file per day)."""
    try:
        ensure_directory_exists(LOG_FOLDER)

        log_date = timestamp.strftime("%Y%m%d")
        log_file = f"{LOG_FOLDER}/ingestion_log_{log_date}.jsonl"

        log_entry = {
            "timestamp": timestamp.isoformat(),
            "location": LOCATION_NAME,
            "success": success,
            "file_path": file_path,
            "error_message": error_message
        }

        # Append mode preserves all log entries from multiple runs
        with open(log_file, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')

    except Exception:
        pass  # Don't crash the job if logging fails

# COMMAND ----------

# DBTITLE 1,Main Execution
def run_ingestion_pipeline():
    """Main pipeline orchestrator: fetch -> save -> log."""
    ingestion_timestamp = get_current_timestamp()

    # Step 1: Fetch data from API
    success, weather_data, error = fetch_weather_data(API_BASE_URL, API_PARAMS)

    if not success:
        write_ingestion_log(ingestion_timestamp, False, error_message=error)
        return False

    # Step 2: Save data with partitioning and metadata
    success, file_path, error = save_weather_data(
        weather_data, DATA_FOLDER, LOCATION_NAME, ingestion_timestamp
    )

    if not success:
        write_ingestion_log(ingestion_timestamp, False, error_message=error)
        return False

    # Step 3: Write success log
    write_ingestion_log(ingestion_timestamp, True, file_path=file_path)

    return True

if __name__ == "__main__":
    run_ingestion_pipeline()