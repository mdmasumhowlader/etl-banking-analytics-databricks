# Data Dictionary

## Bronze/Silver Tables

### DIVISIONS
| Column | Type | Description |
|--------|------|-------------|
| DIVISION_ID | INT | Primary key |
| DIVISION_NAME | STRING | Division name |

### DISTRICTS
| Column | Type | Description |
|--------|------|-------------|
| DISTRICT_ID | INT | Primary key |
| DISTRICT_NAME | STRING | District name |
| DIVISION_ID | INT | Foreign key to DIVISIONS |

### THANAS
| Column | Type | Description |
|--------|------|-------------|
| THANA_ID | INT | Primary key |
| THANA_NAME | STRING | Thana/sub-district name |
| DISTRICT_ID | INT | Foreign key to DISTRICTS |

### BRANCHES
| Column | Type | Description |
|--------|------|-------------|
| BRANCH_ID | INT | Primary key |
| BRANCH_NAME | STRING | Branch name |
| ADDRESS | STRING | Branch address |
| THANA_ID | INT | Foreign key to THANAS |
| LAST_UPDATE_TIME | TIMESTAMP | Last update timestamp |

### BUSINESS_UNITS
| Column | Type | Description |
|--------|------|-------------|
| BUSINESS_UNIT_ID | INT | Primary key |
| BUSINESS_UNIT_NAME | STRING | Business unit name |

### CL_CATEGORIES
| Column | Type | Description |
|--------|------|-------------|
| CL_CATEGORY_ID | INT | Primary key |
| CL_CODE | STRING | Credit/Loan category code |
| DESCRIPTION | STRING | Category description |

### CUSTOMER_TYPES
| Column | Type | Description |
|--------|------|-------------|
| CUSTOMER_TYPE_ID | INT | Primary key |
| CUSTOMER_TYPE | STRING | Customer type |
| DESCRIPTION | STRING | Type description |

### CUSTOMERS
| Column | Type | Description |
|--------|------|-------------|
| CUSTOMER_ID | INT | Primary key |
| CUSTOMER_TYPE_ID | INT | Foreign key to CUSTOMER_TYPES |
| CUSTOMER_NAME | STRING | Customer name |
| CUSTOMER_RISK_LEVEL | STRING | Risk level (LOW/MEDIUM/HIGH) |
| ADDRESS | STRING | Customer address |
| EMAIL | STRING | Email |
| MOBILE | STRING | Mobile number |
| BRANCH_ID | INT | Foreign key to BRANCHES |
| LAST_UPDATE_TIME | TIMESTAMP | Last update timestamp |

### INDUSTRIES
| Column | Type | Description |
|--------|------|-------------|
| INDUSTRY_ID | INT | Primary key |
| SECTOR_NAME | STRING | Sector name |
| SUB_SECTOR_NAME | STRING | Sub-sector name |
| RISK_CATEGORY | STRING | Risk category |

### FINANCING_PRODUCTS
| Column | Type | Description |
|--------|------|-------------|
| PRODUCT_ID | INT | Primary key |
| PRODUCT_CODE | STRING | Product code |
| PRODUCT_NAME | STRING | Product name |
| PRODUCT_TYPE | STRING | Product type (MARKUP/RENT) |
| INSTALLMENT_BASED | INT | 1 if installment-based |
| INDUSTRY_ID | INT | Foreign key to INDUSTRIES |
| PERSONAL_FINANCING | INT | 1 if personal financing |

### FINANCING_ACCOUNTS
| Column | Type | Description |
|--------|------|-------------|
| ACCOUNT_ID | INT | Primary key |
| CUSTOMER_ID | INT | Foreign key to CUSTOMERS |
| BRANCH_ID | INT | Foreign key to BRANCHES |
| BUSINESS_UNIT_ID | INT | Foreign key to BUSINESS_UNITS |
| PRODUCT_ID | INT | Foreign key to FINANCING_PRODUCTS |
| CL_CATEGORY_ID | INT | Foreign key to CL_CATEGORIES |
| ACCOUNT_NO | STRING | Account number |
| OPEN_DATE | DATE | Account opening date |
| EXPIRY_DATE | DATE | Account expiry date |
| CLOSE_DATE | DATE | Account closing date |
| ACCOUNT_STATUS | STRING | ACTIVE/CLOSED/DORMANT |
| SANCTION_AMOUNT | DOUBLE | Sanctioned amount |
| OUTSTANDING_AMT | DOUBLE | Outstanding amount |
| PRINCIPAL_BALANCE | DOUBLE | Principal balance |
| ACC_PROFIT_RATE | FLOAT | Profit rate |
| IS_INSTALLMENT | INT | 1 if installment-based |
| CL_STATUS | STRING | Credit/Loan status |

## Gold Layer (Star Schema)

### DIM_BRANCH
| Column | Type | Description |
|--------|------|-------------|
| BRANCH_SK | INT | Surrogate key (PK) |
| BRANCH_ID | INT | Business key |
| BRANCH_NAME | STRING | Branch name |
| THANA_NAME | STRING | Thana name |
| DISTRICT_NAME | STRING | District name |
| DIVISION_NAME | STRING | Division name |
| START_DATE | DATE | SCD Type 2 start date |
| END_DATE | DATE | SCD Type 2 end date |
| IS_CURRENT | INT | 1 if current |

### DIM_CUSTOMER
| Column | Type | Description |
|--------|------|-------------|
| CUSTOMER_SK | INT | Surrogate key (PK) |
| CUSTOMER_ID | INT | Business key |
| CUSTOMER_NAME | STRING | Customer name |
| CUSTOMER_TYPE | STRING | Customer type |
| CUSTOMER_RISK_LEVEL | STRING | Risk level |
| HIGH_RISK_FLAG | INT | 1 if high risk |
| CUSTOMER_SEGMENT | STRING | INDIVIDUAL/BUSINESS |
| START_DATE | DATE | SCD Type 2 start date |
| END_DATE | DATE | SCD Type 2 end date |
| IS_CURRENT | INT | 1 if current |

### FACT_FINANCING_ACCOUNT
| Column | Type | Description |
|--------|------|-------------|
| ACCOUNT_ID | INT | Primary key |
| CUSTOMER_SK | INT | Foreign key to DIM_CUSTOMER |
| BRANCH_SK | INT | Foreign key to DIM_BRANCH |
| PRODUCT_SK | INT | Foreign key to DIM_PRODUCT |
| INDUSTRY_SK | INT | Foreign key to DIM_INDUSTRY |
| BUSINESS_UNIT_SK | INT | Foreign key to DIM_BUSINESS_UNIT |
| CL_CATEGORY_SK | INT | Foreign key to DIM_CL_CATEGORY |
| ACCOUNT_NO | STRING | Account number |
| SANCTION_AMOUNT | DOUBLE | Sanctioned amount |
| OUTSTANDING_AMT | DOUBLE | Outstanding amount |
| CL_STATUS | STRING | Credit/Loan status |
| BALANCE_DATE | DATE | Reporting date |