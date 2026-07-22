# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from metadata import schema, primary_keys, table_metadata

catalog_name = "inv_risk_mgmt"
bronze_schema = "bronze"
silver_schema = "silver"

def apply_generic_rules(df, table_name, schema):

    pk_cols = primary_keys.get(table_name, [])

    # Deduplicate
    if pk_cols:
        df = df.dropna(subset=pk_cols)
        df = df.dropDuplicates(pk_cols)

    # Standardize strings
    for field in schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.trim(F.upper(F.col(field.name)))
            )

    # Add audit column
    df = df.withColumn("LAST_UPDATE_TIME", F.current_timestamp())
    df = df.drop("ingestion_time")
    df = df.drop("source_file")

    return df


def apply_metadata_rules(df, config, spark):

    # Default values
    if "default_values" in config:
        df = df.fillna(config["default_values"])

    # Column rules
    if "column_rules" in config:
        for col, rule in config["column_rules"].items():
            if rule == "null_to_zero":
                df = df.withColumn(col, F.coalesce(F.col(col), F.lit(0)))
            elif rule == "non_negative":
                df = df.withColumn(col, F.when(F.col(col) < 0, 0).otherwise(F.col(col)))        
            elif rule == "normalize_risk_category":
                df = df.withColumn(
                    col,
                    F.when(F.upper(F.col(col)).like("%LOW%"), "LOW")
                     .when(F.upper(F.col(col)).like("%MEDIUM%"), "MEDIUM")
                     .when(F.upper(F.col(col)).like("%HIGH%"), "HIGH")
                     .otherwise("UNKNOWN")
                )

    # Joins
    if "joins" in config:
        for j in config["joins"]:
            ref_df = spark.table(f"{catalog_name}.{silver_schema}.{j['table']}")
            df = df.join(ref_df, on=j["key"], how=j["type"])

    return df


def apply_business_logic(df, rules):

    if "npl_flag" in rules:
        df = df.withColumn(
            "IS_NPL",
            F.when(F.col("CL_STATUS").isin("SS", "DF", "BL"), 1).otherwise(0)
        )

    if "utilization" in rules:
        df = df.withColumn(
            "UTILIZATION_RATIO",
            F.col("OUTSTANDING_AMT") / F.col("SANCTION_AMOUNT")
        )

    if "customer_segment" in rules:
        df = df.withColumn(
            "CUSTOMER_SEGMENT",
            F.when(F.col("CUSTOMER_TYPE").isin("PERSONAL", "PERSONAL JOINT"), "INDIVIDUAL")
             .otherwise("BUSINESS")
        )

    if "product_category" in rules:
        df = df.withColumn(
            "PRODUCT_CATEGORY",
            F.when(F.col("PRODUCT_TYPE") == "MARKUP", "TRADE_FINANCE")
             .when(F.col("PRODUCT_TYPE") == "RENT", "LEASE_FINANCE")
             .otherwise("OTHER")
        )

    if "loan_category_group" in rules:
        df = df.withColumn(
            "LOAN_CATEGORY_GROUP",
            F.when(F.col("DESCRIPTION").like("%CONTINUOUS%"), "REVOLVING")
             .when(F.col("DESCRIPTION").like("%TERM%"), "LONG_TERM")
             .otherwise("OTHER")
        )

    if "high_risk_flag" in rules:
        df = df.withColumn(
            "HIGH_RISK_FLAG",
            F.when(F.col("CUSTOMER_RISK_LEVEL") == "HIGH", 1).otherwise(0)
        )

    return df

def check_pk_duplicates(df, table_name, pk_cols):

    if not pk_cols:
        return

    dup_df = (
        df.groupBy(pk_cols)
          .count()
          .filter("count > 1")
    )

    if dup_df.count() > 0:
        print(f"Duplicate PK found in {table_name}")
        dup_df.show()

def process_table(table_name):

    print(f"Processing {table_name}...")

    df = spark.table(f"{catalog_name}.{bronze_schema}.{table_name}")

    # Step 1: Generic rules
    df = apply_generic_rules(df, table_name, schema[table_name])

    # Step 2: Data quality
    # pk_cols = primary_keys.get(table_name, [])
    # check_pk_duplicates(df, table_name, pk_cols)

    # Step 3: Metadata rules
    if table_name in table_metadata:
        config = table_metadata[table_name]

        df = apply_metadata_rules(df, config, spark)

        # Step 4: Business logic
        if "business_logic" in config:
            df = apply_business_logic(df, config["business_logic"])

    # Step 5: Write
    df.write \
        .format("delta")\
        .option("overwriteSchema", "true")\
        .mode("overwrite")\
        .saveAsTable(
        f"{catalog_name}.{silver_schema}.{table_name}"
    )


# Run all tables
for table_name in schema.keys():
    process_table(table_name)





