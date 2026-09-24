# Databricks notebook source
# DBTITLE 1,Configuration
# Configuration
LANDING_PATH = "/Volumes/my_project_catalog/weather_schema/landing_stage/weather"
BRONZE_TABLE = "my_project_catalog.weather_schema.bronze_weather_raw"

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

# DBTITLE 1,MERGE into Delta Table (Incremental Insert)
# INCREMENTAL: Only insert NEW files not already in bronze table.
# Uses MERGE on source_file to skip already-loaded files - no duplicates.
df_bronze.createOrReplaceTempView("bronze_updates")

spark.sql(f"""
    MERGE INTO {BRONZE_TABLE} AS t
    USING bronze_updates AS s
    ON t.source_file = s.source_file
    WHEN NOT MATCHED THEN INSERT *
""")

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

print(f"Bronze table row count: {row_count}")
