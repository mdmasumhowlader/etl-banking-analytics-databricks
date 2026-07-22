# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------


from pyspark.sql import functions as F
from delta.tables import DeltaTable
from metadata import tables, schema
from pyspark.sql.types import DateType, TimestampType

catelog_name = "inv_risk_mgmt"
schema_name = "bronze"
base_path = "/Volumes/inv_risk_mgmt/raw_data/csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catelog_name}.{schema_name}")

load_date = "2026-03-18"
csv_paths = {tbl: f"{base_path}/{load_date}/{tbl}.csv" for tbl in tables}

def load_csv(spark, file_path:str, schema):
    df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", schema is None)\
        .option("timestampsFormat", "M/d/yyyy h:mm:ss.SSSSSS a")\
        .schema(schema)\
        .load(file_path)
    return df

for tbl in tables:
    print(f"Loading {tbl}...")
    df = load_csv(spark, csv_paths[tbl], schema[tbl])

    tble_name = spark.table(f"{catelog_name}.{schema_name}.{tbl}")
    for fld in tble_name.schema.fields:
        col_name = fld.name.upper()
        #print(col_name)
        if col_name not in df.columns:
            continue

        if col_name == 'CREATED_AT':
            df = df.withColumn(
                col_name,
                F.to_timestamp(F.col(col_name), "M/d/yyyy h:mm:ss a")
            )
        elif isinstance(fld.dataType, DateType):
            df = df.withColumn(
                col_name,
                F.to_date(F.col(col_name), "M/d/yyyy")
            )
        elif isinstance(fld.dataType, TimestampType):
            df = df.withColumn(
                col_name,
                    F.to_timestamp(F.col(col_name),"M/d/yyyy h:mm:ss.SSSSSS a")
            )

    df = df.withColumn("ingestion_time", F.current_timestamp())\
        .withColumn("source_file", F.lit(csv_paths[tbl]))

    df.write.mode("overwrite").saveAsTable(f"{catelog_name}.{schema_name}.{tbl}")
    #df.display()
    print(f"{tbl} loaded.")

