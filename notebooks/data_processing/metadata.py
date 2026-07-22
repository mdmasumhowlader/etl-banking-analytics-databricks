from pyspark.sql.types import * 

tables = [
    "DIVISIONS",
    "DISTRICTS",
    "THANAS",
    "BRANCHES",
    "BUSINESS_UNITS",
    "CL_CATEGORIES",
    "CUSTOMER_TYPES",
    "INDUSTRIES",
    "CUSTOMERS",
    "FINANCING_PRODUCTS",
    "FINANCING_ACCOUNTS"
]

primary_keys = {
    "DIVISIONS": ["DIVISION_ID"],
    "DISTRICTS": ["DISTRICT_ID"],
    "THANAS": ["THANA_ID"],
    "BRANCHES": ["BRANCH_ID"],
    "BUSINESS_UNITS": ["BUSINESS_UNIT_ID"],
    "CUSTOMERS": ["CUSTOMER_ID"],
    "CUSTOMER_TYPES": ["CUSTOMER_TYPE_ID"],
    "INDUSTRIES": ["INDUSTRY_ID"],
    "FINANCING_PRODUCTS": ["PRODUCT_ID"],
    "CL_CATEGORIES": ["CL_CATEGORY_ID"],
    "FINANCING_ACCOUNTS": ["ACCOUNT_ID"]
}

divisions_schema = StructType([
    StructField("DIVISION_ID", IntegerType(), False),
    StructField("DIVISION_NAME", StringType(), False)
])

districts_schema = StructType([
    StructField("DISTRICT_ID", IntegerType(), False),
    StructField("DISTRICT_NAME", StringType(), False),
    StructField("DIVISION_ID", IntegerType(), False)
])

thanas_schema = StructType([
    StructField("THANA_ID", IntegerType(), False),
    StructField("THANA_NAME", StringType(), False),
    StructField("DISTRICT_ID", IntegerType(), False)
])

branches_schema = StructType([
    StructField("BRANCH_ID", IntegerType(), False),
    StructField("BRANCH_NAME", StringType(), False),
    StructField("ADDRESS", StringType(), True),
    StructField("THANA_ID", IntegerType(), True),
    StructField("LAST_UPDATE_TIME", StringType(),False)
])

business_units_schema = StructType([
    StructField("BUSINESS_UNIT_ID", IntegerType(), False),
    StructField("BUSINESS_UNIT_NAME", StringType(), False)
])

cl_categories_schema = StructType([
    StructField("CL_CATEGORY_ID", IntegerType(), False),
    StructField("CL_CODE", StringType(), False),
    StructField("DESCRIPTION", StringType(), False)
])

customer_types_schema = StructType([
    StructField("CUSTOMER_TYPE_ID", IntegerType(), False),
    StructField("CUSTOMER_TYPE", StringType(), False),
    StructField("DESCRIPTION", StringType(), False)
])

industries_schema = StructType([
    StructField("INDUSTRY_ID", IntegerType(), False),
    StructField("SECTOR_NAME", StringType(), False),
    StructField("SUB_SECTOR_NAME", StringType(), True),
    StructField("RISK_CATEGORY", StringType(), True)
])

customers_schema = StructType([
    StructField("CUSTOMER_ID", IntegerType(), False),
    StructField("CUSTOMER_TYPE_ID", IntegerType(), True),
    StructField("CUSTOMER_NAME", StringType(), False),
    StructField("CUSTOMER_RISK_LEVEL", StringType(), True),
    StructField("LAST_UPDATE_TIME", StringType(),False),
    StructField("ADDRESS", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("MOBILE", StringType(), True),
    StructField("BRANCH_ID", IntegerType(), True)
])

financing_products_schema = StructType([
    StructField("PRODUCT_ID", IntegerType(), False),
    StructField("PRODUCT_CODE", StringType(), False),
    StructField("PRODUCT_NAME", StringType(), False),
    StructField("PRODUCT_TYPE", StringType(), True),
    StructField("INSTALLMENT_BASED", IntegerType(), True),
    StructField("INDUSTRY_ID", IntegerType(), True),
    StructField("LAST_UPDATE_TIME", StringType(),False),
    StructField("PERSONAL_FINANCING", IntegerType(), True)
])

financing_accounts_schema = StructType([
    StructField("ACCOUNT_ID", IntegerType(),False),
    StructField("CUSTOMER_ID", IntegerType(),False),
    StructField("BRANCH_ID", IntegerType(),False),
    StructField("BUSINESS_UNIT_ID", IntegerType(),True),
    StructField("PRODUCT_ID", IntegerType(),False),
    StructField("CL_CATEGORY_ID", IntegerType(),True),
    StructField("ACCOUNT_NO", StringType(),False),
    StructField("OPEN_DATE", StringType(),True),
    StructField("EXPIRY_DATE", StringType(),True),
    StructField("CLOSE_DATE", StringType(),True),
    StructField("ACCOUNT_STATUS", StringType(),True),
    StructField("SANCTION_AMOUNT", DoubleType(),True),
    StructField("OUTSTANDING_AMT", DoubleType(),False),
    StructField("PRINCIPAL_BALANCE", DoubleType(),True),
    StructField("ACC_PROFIT_RATE", FloatType(),True),
    StructField("IS_INSTALLMENT", IntegerType(),True),
    StructField("CL_STATUS", StringType(),True),
    StructField("CREATED_AT", StringType(),False),
    StructField("LAST_UPDATE_TIME", StringType(),False)
])

schema = {
    "DIVISIONS" : divisions_schema,
    "DISTRICTS" : districts_schema,
    "THANAS" : thanas_schema,
    "BRANCHES" : branches_schema,
    "BUSINESS_UNITS" : business_units_schema,
    "CL_CATEGORIES" : cl_categories_schema,
    "CUSTOMER_TYPES" : customer_types_schema,
    "INDUSTRIES" : industries_schema,
    "CUSTOMERS" : customers_schema,
    "FINANCING_PRODUCTS" : financing_products_schema,
    "FINANCING_ACCOUNTS" : financing_accounts_schema
}

table_metadata = {
    "CUSTOMERS": {
        "default_values": {
            "CUSTOMER_RISK_LEVEL": "UNKNOWN"
        },
        "joins": [
            {"table": "CUSTOMER_TYPES", "key": "CUSTOMER_TYPE_ID", "type": "left_semi"},
            {"table": "BRANCHES", "key": "BRANCH_ID", "type": "left_semi"}
        ],
        "business_logic": ["high_risk_flag"]
    },

    "INDUSTRIES": {
        "column_rules": {
           "RISK_CATEGORY": "normalize_risk_category"
        }
    },

    "FINANCING_ACCOUNTS": {
        "default_values": {
            "ACCOUNT_STATUS": "ACTIVE"
        },        
        "column_rules": {
            "OUTSTANDING_AMT": "non_negative"
        },     
        "business_logic": ["npl_flag", "utilization"]
    },

    "FINANCING_PRODUCTS": {
        "business_logic": ["product_category"]
    },

    "CUSTOMER_TYPES": {
        "business_logic": ["customer_segment"]
    },

    "CL_CATEGORIES": {
        "business_logic": ["loan_category_group"]
    }
}


dimension_config = {
    "dim_branch": {"unknown": True, "na": False},
    "dim_financing_product": {"unknown": True, "na": False, "column_rules": {"INDUSTRY_ID": "null_to_na"},
        "column_overrides": {
            "unknown": {
                "INDUSTRY_ID": -1
            }
        }},
    "dim_industry": {"unknown": True, "na": True},   
    "dim_business_unit": {"unknown": True, "na": True}, 
    "dim_cl_category": {"unknown": True, "na": False},
    "dim_customer": {"unknown": True, "na": False}
}