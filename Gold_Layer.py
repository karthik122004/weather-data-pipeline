# Databricks notebook source
# DBTITLE 1,Configuration
# Configuration
SILVER_TABLE_HOURLY = "my_project_catalog.weather_schema.silver_weather_hourly"
SILVER_TABLE_DAILY = "my_project_catalog.weather_schema.silver_weather_daily"
GOLD_DAILY_SUMMARY = "my_project_catalog.weather_schema.gold_daily_weather_summary"
GOLD_WEEKLY_SUMMARY = "my_project_catalog.weather_schema.gold_weekly_weather_summary"

# COMMAND ----------

# DBTITLE 1,Read Silver Tables
# Read Silver tables
df_silver_hourly = spark.table(SILVER_TABLE_HOURLY)
df_silver_daily = spark.table(SILVER_TABLE_DAILY)

# COMMAND ----------

# DBTITLE 1,Daily Summary Aggregation
from pyspark.sql.functions import (
    col, avg, min, max, sum as spark_sum, count,
    round as spark_round, to_date
)

# Extract date from timestamp for daily grouping
df_hourly_with_date = df_silver_hourly \
    .filter(col("is_valid_temperature") == True) \
    .withColumn("observation_date", to_date(col("observation_timestamp")))

# Aggregate hourly data into daily summaries
df_daily_summary = df_hourly_with_date.groupBy("location", "observation_date").agg(
    spark_round(avg("temperature_fahrenheit"), 1).alias("avg_temperature"),
    spark_round(min("temperature_fahrenheit"), 1).alias("min_temperature"),
    spark_round(max("temperature_fahrenheit"), 1).alias("max_temperature"),
    spark_round(avg("humidity_percent"), 1).alias("avg_humidity"),
    spark_round(min("humidity_percent"), 1).alias("min_humidity"),
    spark_round(max("humidity_percent"), 1).alias("max_humidity"),
    spark_round(spark_sum("precipitation_inches"), 2).alias("total_precipitation"),
    count("*").alias("num_observations")
).orderBy("observation_date")

# COMMAND ----------

# DBTITLE 1,Window Functions for Trends
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, row_number

# Window for day-over-day comparisons
window_spec = Window.partitionBy("location").orderBy("observation_date")

# Add trend columns: previous day temp, next day temp, day-over-day change
df_daily_with_trends = df_daily_summary \
    .withColumn("prev_day_avg_temp", lag("avg_temperature", 1).over(window_spec)) \
    .withColumn("next_day_avg_temp", lead("avg_temperature", 1).over(window_spec)) \
    .withColumn("temp_change_from_prev_day",
        spark_round(col("avg_temperature") - col("prev_day_avg_temp"), 1)) \
    .withColumn("day_number", row_number().over(window_spec))

# COMMAND ----------

# DBTITLE 1,Weekly Aggregates
from pyspark.sql.functions import weekofyear, year, date_trunc

# Add week grouping columns
df_daily_with_week = df_daily_with_trends \
    .withColumn("week_start_date", date_trunc("week", col("observation_date"))) \
    .withColumn("year", year(col("observation_date"))) \
    .withColumn("week_of_year", weekofyear(col("observation_date")))

# Roll up daily data into weekly summaries
df_weekly_summary = df_daily_with_week.groupBy(
    "location", "year", "week_of_year", "week_start_date"
).agg(
    spark_round(avg("avg_temperature"), 1).alias("weekly_avg_temperature"),
    spark_round(min("min_temperature"), 1).alias("weekly_min_temperature"),
    spark_round(max("max_temperature"), 1).alias("weekly_max_temperature"),
    spark_round(avg("avg_humidity"), 1).alias("weekly_avg_humidity"),
    spark_round(spark_sum("total_precipitation"), 2).alias("weekly_total_precipitation"),
    count("observation_date").alias("num_days_in_week")
).orderBy("week_start_date")

# COMMAND ----------

# DBTITLE 1,Business Metrics & Flags
from pyspark.sql.functions import when, current_timestamp

# Add business logic: flags, categories, and derived metrics
df_gold_daily = df_daily_with_trends \
    .withColumn("is_hot_day", when(col("max_temperature") >= 95, True).otherwise(False)) \
    .withColumn("is_rainy_day", when(col("total_precipitation") > 0.1, True).otherwise(False)) \
    .withColumn("temperature_range",
        spark_round(col("max_temperature") - col("min_temperature"), 1)) \
    .withColumn("temperature_category",
        when(col("avg_temperature") >= 90, "Very Hot")
        .when(col("avg_temperature") >= 80, "Hot")
        .when(col("avg_temperature") >= 70, "Warm")
        .when(col("avg_temperature") >= 60, "Mild")
        .otherwise("Cool")) \
    .withColumn("gold_processing_timestamp", current_timestamp())

# COMMAND ----------

# DBTITLE 1,Write Gold Tables
# Select final columns and write daily Gold table
df_gold_daily_select = df_gold_daily.select(
    "location", "observation_date", "day_number",
    "avg_temperature", "min_temperature", "max_temperature",
    "temperature_range", "temperature_category", "temp_change_from_prev_day",
    "avg_humidity", "min_humidity", "max_humidity",
    "total_precipitation",
    "is_hot_day", "is_rainy_day",
    "num_observations", "gold_processing_timestamp"
)

df_gold_daily_select.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_DAILY_SUMMARY)

# Write weekly Gold table
df_weekly_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_WEEKLY_SUMMARY)

# COMMAND ----------

# DBTITLE 1,Business Queries
# Top 3 hottest days
spark.sql(f"""
    SELECT observation_date, avg_temperature, max_temperature,
           temperature_category, is_hot_day
    FROM {GOLD_DAILY_SUMMARY}
    ORDER BY max_temperature DESC
    LIMIT 3
""").show(truncate=False)

# Biggest day-over-day temperature changes
spark.sql(f"""
    SELECT observation_date, day_number, avg_temperature, temp_change_from_prev_day
    FROM {GOLD_DAILY_SUMMARY}
    WHERE temp_change_from_prev_day IS NOT NULL
    ORDER BY ABS(temp_change_from_prev_day) DESC
    LIMIT 3
""").show(truncate=False)

# Rainy days
spark.sql(f"""
    SELECT observation_date, total_precipitation, avg_temperature, avg_humidity
    FROM {GOLD_DAILY_SUMMARY}
    WHERE is_rainy_day = true
    ORDER BY total_precipitation DESC
""").show(truncate=False)

# Weekly rollup
spark.sql(f"""
    SELECT week_start_date, weekly_avg_temperature,
           weekly_min_temperature, weekly_max_temperature,
           weekly_total_precipitation
    FROM {GOLD_WEEKLY_SUMMARY}
    ORDER BY week_start_date
""").show(truncate=False)