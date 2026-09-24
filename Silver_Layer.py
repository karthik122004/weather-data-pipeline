# Databricks notebook source
# DBTITLE 1,Configuration
# Configuration
BRONZE_TABLE = "my_project_catalog.weather_schema.bronze_weather_raw"
SILVER_TABLE_HOURLY = "my_project_catalog.weather_schema.silver_weather_hourly"
SILVER_TABLE_DAILY = "my_project_catalog.weather_schema.silver_weather_daily"

# COMMAND ----------

# DBTITLE 1,Read Bronze Table
# Read Bronze table
df_bronze = spark.table(BRONZE_TABLE)
df_bronze.printSchema()

# COMMAND ----------

# DBTITLE 1,Transform Hourly Data
from pyspark.sql.functions import (
    explode, arrays_zip, col, to_timestamp, current_timestamp
)

# Zip parallel arrays together, then explode into individual rows
# Before: 1 row with arrays[168] -> After: 168 rows with flat columns
df_hourly_zipped = df_bronze.select(
    col("metadata.location").alias("location"),
    col("metadata.ingestion_timestamp").alias("ingestion_timestamp"),
    col("load_timestamp"),
    arrays_zip(
        col("raw_data.hourly.time"),
        col("raw_data.hourly.temperature_2m"),
        col("raw_data.hourly.relative_humidity_2m"),
        col("raw_data.hourly.precipitation"),
        col("raw_data.hourly.precipitation_probability"),
        col("raw_data.hourly.weather_code")
    ).alias("hourly_data")
)

# Explode array of structs into rows
df_hourly_exploded = df_hourly_zipped.select(
    "location", "ingestion_timestamp", "load_timestamp",
    explode("hourly_data").alias("hourly_record")
)

# Flatten struct fields into top-level columns
df_hourly_flat = df_hourly_exploded.select(
    "location", "ingestion_timestamp", "load_timestamp",
    col("hourly_record.time").alias("observation_time"),
    col("hourly_record.temperature_2m").alias("temperature_fahrenheit"),
    col("hourly_record.relative_humidity_2m").alias("humidity_percent"),
    col("hourly_record.precipitation").alias("precipitation_inches"),
    col("hourly_record.precipitation_probability").alias("precipitation_probability_percent"),
    col("hourly_record.weather_code").alias("weather_code")
)

df_hourly_flat.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Data Quality & Cleaning
from pyspark.sql.functions import to_timestamp, when

# Convert string timestamp to proper TIMESTAMP type
df_hourly_cleaned = df_hourly_flat \
    .withColumn("observation_timestamp",
        to_timestamp(col("observation_time"), "yyyy-MM-dd'T'HH:mm")) \
    .drop("observation_time")

# Add data quality validation flags
df_hourly_quality = df_hourly_cleaned \
    .withColumn("is_valid_temperature",
        when((col("temperature_fahrenheit").isNotNull()) &
             (col("temperature_fahrenheit") >= -50) &
             (col("temperature_fahrenheit") <= 130), True)
        .otherwise(False)) \
    .withColumn("is_valid_humidity",
        when((col("humidity_percent").isNotNull()) &
             (col("humidity_percent") >= 0) &
             (col("humidity_percent") <= 100), True)
        .otherwise(False)) \
    .withColumn("silver_processing_timestamp", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write Silver Hourly Table
# Select final columns and write to Delta
df_silver_hourly = df_hourly_quality.select(
    "location", "observation_timestamp",
    "temperature_fahrenheit", "humidity_percent",
    "precipitation_inches", "precipitation_probability_percent",
    "weather_code",
    "is_valid_temperature", "is_valid_humidity",
    "ingestion_timestamp", "load_timestamp", "silver_processing_timestamp"
)

df_silver_hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE_HOURLY)

# COMMAND ----------

# DBTITLE 1,Write Silver Daily Table
# Same explode pattern for daily forecast data
df_daily_zipped = df_bronze.select(
    col("metadata.location").alias("location"),
    col("load_timestamp"),
    arrays_zip(
        col("raw_data.daily.time"),
        col("raw_data.daily.temperature_2m_max"),
        col("raw_data.daily.temperature_2m_min"),
        col("raw_data.daily.precipitation_sum"),
        col("raw_data.daily.weather_code")
    ).alias("daily_data")
)

df_daily_flat = df_daily_zipped.select(
    "location", "load_timestamp",
    explode("daily_data").alias("daily_record")
).select(
    "location", "load_timestamp",
    to_timestamp(col("daily_record.time"), "yyyy-MM-dd").alias("forecast_date"),
    col("daily_record.temperature_2m_max").alias("max_temperature_fahrenheit"),
    col("daily_record.temperature_2m_min").alias("min_temperature_fahrenheit"),
    col("daily_record.precipitation_sum").alias("total_precipitation_inches"),
    col("daily_record.weather_code").alias("weather_code"),
    current_timestamp().alias("silver_processing_timestamp")
)

df_daily_flat.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE_DAILY)

# COMMAND ----------

# DBTITLE 1,Verify Silver Tables
# Verify Silver tables
spark.sql(f"""
    SELECT observation_timestamp, temperature_fahrenheit, humidity_percent,
           precipitation_inches, is_valid_temperature
    FROM {SILVER_TABLE_HOURLY}
    ORDER BY observation_timestamp DESC
    LIMIT 5
""").show(truncate=False)

spark.sql(f"""
    SELECT forecast_date, max_temperature_fahrenheit, min_temperature_fahrenheit,
           total_precipitation_inches
    FROM {SILVER_TABLE_DAILY}
    ORDER BY forecast_date
""").show(truncate=False)