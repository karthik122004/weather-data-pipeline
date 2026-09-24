# Databricks notebook source
# DBTITLE 1,Configuration
# Configuration
LANDING_PATH = "/Volumes/my_project_catalog/weather_schema/landing_stage/weather"
BRONZE_TABLE = "my_project_catalog.weather_schema.bronze_weather_raw"

# COMMAND ----------

# DBTITLE 1,Define Schema
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

# Define schema explicitly for performance and data safety
schema = StructType([
    StructField("metadata", StructType([
        StructField("ingestion_timestamp", StringType(), True),
        StructField("source_api", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("ingestion_version", StringType(), True)
    ]), True),
    StructField("raw_data", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("elevation", DoubleType(), True)
    ]), True)
])

# COMMAND ----------

# DBTITLE 1,Read JSON Files
# Read JSON files recursively with multiline support (lazy evaluation)
df_raw = spark.read \
    .option("recursiveFileLookup", "true") \
    .option("multiLine", "true") \
    .json(LANDING_PATH)

df_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Preview Data
# Preview data before writing to Delta
df_raw.show(2, truncate=False, vertical=True)

# COMMAND ----------

# DBTITLE 1,Add Technical Columns
from pyspark.sql.functions import current_timestamp, col, lit

# Add pipeline metadata columns for lineage tracking
df_bronze = df_raw \
    .withColumn("load_timestamp", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn("bronze_version", lit("1.0"))

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
# Write to Delta table (ACID transactions, time travel, schema enforcement)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(BRONZE_TABLE)

# COMMAND ----------

# DBTITLE 1,Verify Delta Table
# Verify the Delta table
df_verify = spark.table(BRONZE_TABLE)
row_count = df_verify.count()

df_verify.select(
    "metadata.location",
    "metadata.ingestion_timestamp",
    "load_timestamp",
    "source_file"
).show(5, truncate=80)