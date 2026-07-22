# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from metadata import dimension_config

catalog_name = "inv_risk_mgmt"
silver_schema = f"{catalog_name}.silver"
gold_schema = f"{catalog_name}.gold"

load_date = "2026-03-31" 

def add_scd2_columns(df, sk_col, nk_cols):

    window_spec = Window.orderBy(nk_cols)
    df = df.withColumn(sk_col, F.row_number().over(window_spec))

    df = df.withColumn("START_DATE", F.lit(load_date).cast("date")) \
           .withColumn("END_DATE", F.lit("9999-12-31").cast("date")) \
           .withColumn("IS_CURRENT", F.lit(1))

    exclude_cols = set(nk_cols + [sk_col, "START_DATE", "END_DATE", "IS_CURRENT"])
    
    attr_cols = [c for c in df.columns if c not in exclude_cols]

    df = df.select(
        sk_col,
        *nk_cols,
        *attr_cols,
        "START_DATE",
        "END_DATE",
        "IS_CURRENT"
    )

    return df

def add_default_dimension_rows(
    df,
    sk_col,
    nk_cols,
    unknown_sk=-1,
    na_sk=-2,
    add_unknown=True,
    add_na=False,
    load_date="2026-03-31",
    column_overrides=None
):
    
    if column_overrides is None:
        column_overrides = {}

    cols = df.columns
    dtype_map = dict(df.dtypes)

    # Identify attribute columns
    exclude_cols = set(nk_cols + [sk_col, "START_DATE", "END_DATE", "IS_CURRENT"])
    attr_cols = [c for c in cols if c not in exclude_cols]

    def build_row(sk_value, label, override_key):
        row_expr = []

        overrides = column_overrides.get(override_key, {})

        for c in cols:
            dtype = dtype_map[c]

            if c in overrides:
                row_expr.append(F.lit(overrides[c]).cast(dtype).alias(c))

            elif c == sk_col:
                row_expr.append(F.lit(sk_value).alias(c))

            elif c in nk_cols:
                row_expr.append(F.lit(sk_value).alias(c))  # keep NK = -1 / -2

            elif c == "START_DATE":
                row_expr.append(F.lit(load_date).cast("date").alias(c))

            elif c == "END_DATE":
                row_expr.append(F.lit("9999-12-31").cast("date").alias(c))

            elif c == "IS_CURRENT":
                row_expr.append(F.lit(1).alias(c))

            else:
                dtype = dict(df.dtypes)[c]

                if dtype in ["string"]:
                    row_expr.append(F.lit(label).alias(c))
                elif dtype in ["int", "bigint", "double", "float"]:
                    row_expr.append(F.lit(0).cast(dtype).alias(c))
                else:
                    row_expr.append(F.lit(None).cast(dtype).alias(c))

        return df.select(row_expr).limit(1)
    
    result_df = df

    if add_unknown:
        unknown_df = build_row(unknown_sk, "UNKNOWN", "unknown")
        result_df = result_df.unionByName(unknown_df)

    if add_na:
        na_df = build_row(na_sk, "NOT APPLICABLE", "na")
        result_df = result_df.unionByName(na_df)

    return result_df


def save_dimension(df, table_name, sk_col, nk_cols):

    config = dimension_config[table_name]

    if "column_rules" in config:
        for col, rule in config.get("column_rules").items():
            if rule == "null_to_na":
                df = df.withColumn(col, F.coalesce(F.col(col), F.lit(-2)))

    df = add_scd2_columns(df, sk_col, nk_cols)

    df = add_default_dimension_rows(
        df,
        sk_col,
        nk_cols,
        add_unknown=config.get("unknown", True),
        add_na=config.get("na", False),
        load_date=load_date,
        column_overrides=config.get("column_overrides", {})
    )

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true")\
        .saveAsTable(f"{gold_schema}.{table_name}")

    return df

dim_branch = spark.table(f"{silver_schema}.BRANCHES").alias("b") \
    .join(spark.table(f"{silver_schema}.THANAS").alias("t"), F.col("b.THANA_ID") == F.col("t.THANA_ID"), "inner") \
    .join(spark.table(f"{silver_schema}.DISTRICTS").alias("ds"), F.col("t.DISTRICT_ID") == F.col("ds.DISTRICT_ID"), "inner") \
    .join(spark.table(f"{silver_schema}.DIVISIONS").alias("dv"), F.col("ds.DIVISION_ID") == F.col("dv.DIVISION_ID"), "inner") \
    .select(
        "BRANCH_ID",
        "BRANCH_NAME",
        "THANA_NAME",
        "DISTRICT_NAME",
        "DIVISION_NAME"
    )

dim_branch = save_dimension(dim_branch, "dim_branch", "BRANCH_SK", ["BRANCH_ID"])

dim_product = spark.table(f"{silver_schema}.FINANCING_PRODUCTS")\
    .alias("p")\
    .select(
        "PRODUCT_ID",
        "PRODUCT_CODE",
        "PRODUCT_NAME",
        "PRODUCT_TYPE",
        "INDUSTRY_ID",
        "PERSONAL_FINANCING",
        "INSTALLMENT_BASED",
        "PRODUCT_CATEGORY")

dim_product = save_dimension(dim_product, "dim_financing_product", "PRODUCT_SK", ["PRODUCT_ID"])

dim_industry = spark.table(f"{silver_schema}.INDUSTRIES")\
    .alias("i")\
    .select(
        "INDUSTRY_ID",
        "SECTOR_NAME",
        "SUB_SECTOR_NAME",
        "RISK_CATEGORY"
    )

dim_industry = save_dimension(dim_industry, "dim_industry", "INDUSTRY_SK", ["INDUSTRY_ID"])

dim_business_unit = spark.table(f"{silver_schema}.BUSINESS_UNITS")\
    .alias("bu")\
    .select(
        "BUSINESS_UNIT_ID",
        "BUSINESS_UNIT_NAME")

dim_business_unit = save_dimension(dim_business_unit, "dim_business_unit", "BUSINESS_UNIT_SK", ["BUSINESS_UNIT_ID"])

dim_cl_category = spark.table(f"{silver_schema}.CL_CATEGORIES")\
    .alias("cc")\
    .select(
        "CL_CATEGORY_ID",
        "CL_CODE",
        "DESCRIPTION",
        "LOAN_CATEGORY_GROUP")

dim_cl_category = save_dimension(dim_cl_category, "dim_cl_category", "CL_CATEGORY_SK", ["CL_CATEGORY_ID"])

dim_customer = spark.table(f"{silver_schema}.CUSTOMERS")\
    .alias("c")\
    .join(spark.table(f"{silver_schema}.CUSTOMER_TYPES").alias("ct"), F.col("c.CUSTOMER_TYPE_ID") == F.col("ct.CUSTOMER_TYPE_ID"), "inner")\
    .select(
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
        "c.CUSTOMER_TYPE_ID",
        "CUSTOMER_TYPE",
        "CUSTOMER_RISK_LEVEL",
        "HIGH_RISK_FLAG",
        "CUSTOMER_SEGMENT")
    
dim_customer = save_dimension(dim_customer, "dim_customer", "CUSTOMER_SK", ["CUSTOMER_ID"])

# Fact Table 
dim_customer = spark.table(f"{gold_schema}.dim_customer") \
    .filter("IS_CURRENT = 1") \
    .select("CUSTOMER_ID", "CUSTOMER_SK")

dim_industry = spark.table(f"{gold_schema}.dim_industry") \
    .filter("IS_CURRENT = 1") \
    .select("INDUSTRY_ID", "INDUSTRY_SK")

dim_product = spark.table(f"{gold_schema}.dim_financing_product") \
    .join(dim_industry, on="INDUSTRY_ID", how="left") \
    .filter("IS_CURRENT = 1") \
    .select("PRODUCT_ID", "PRODUCT_SK", "INDUSTRY_SK", "PERSONAL_FINANCING")

dim_branch = spark.table(f"{gold_schema}.dim_branch") \
    .filter("IS_CURRENT = 1") \
    .select("BRANCH_ID", "BRANCH_SK")

dim_bu = spark.table(f"{gold_schema}.dim_business_unit") \
    .filter("IS_CURRENT = 1") \
    .select("BUSINESS_UNIT_ID", "BUSINESS_UNIT_SK")

dim_cl = spark.table(f"{gold_schema}.dim_cl_category") \
    .filter("IS_CURRENT = 1") \
    .select("CL_CATEGORY_ID", "CL_CATEGORY_SK")

fact_df = spark.table(f"{silver_schema}.FINANCING_ACCOUNTS")

fact_df = fact_df \
    .join(dim_customer, on="CUSTOMER_ID", how="left") \
    .join(dim_branch, on="BRANCH_ID", how="left") \
    .join(dim_product, on="PRODUCT_ID", how="left") \
    .join(dim_bu, on="BUSINESS_UNIT_ID", how="left") \
    .join(dim_cl, on="CL_CATEGORY_ID", how="left") \
    .select(
        "ACCOUNT_ID",
        F.coalesce(F.col("CUSTOMER_SK"), F.lit(-1)).alias("CUSTOMER_SK"),
        F.coalesce(F.col("BRANCH_SK"), F.lit(-1)).alias("BRANCH_SK"),
        F.coalesce(F.col("PRODUCT_SK"), F.lit(-1)).alias("PRODUCT_SK"),
        F.coalesce(F.col("INDUSTRY_SK"), F.lit(-1)).alias("INDUSTRY_SK"),
        F.when(F.col("BUSINESS_UNIT_SK").isNotNull(), F.col("BUSINESS_UNIT_SK"))\
        .when(F.col("PERSONAL_FINANCING") == 1, F.lit(-2))\
        .otherwise(F.lit(-1))\
        .alias("BUSINESS_UNIT_SK"),
        F.coalesce(F.col("CL_CATEGORY_SK"), F.lit(-1)).alias("CL_CATEGORY_SK"),
        "ACCOUNT_NO",
        "OPEN_DATE",
        "EXPIRY_DATE",
        "CLOSE_DATE",
        "ACCOUNT_STATUS",
        "SANCTION_AMOUNT",
        "OUTSTANDING_AMT",
        "PRINCIPAL_BALANCE",
        "ACC_PROFIT_RATE",
        "IS_INSTALLMENT",
        "CL_STATUS",
        "CREATED_AT",
        "LAST_UPDATE_TIME"
    ) \
    .withColumn("BALANCE_DATE", F.lit(load_date).cast("date"))

# Save to gold layer
fact_df.write \
    .mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{gold_schema}.FACT_FINANCING_ACCOUNT")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inv_risk_mgmt.gold.FACT_FINANCING_ACCOUNT;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inv_risk_mgmt.bronze.FINANCING_PRODUCTS