## 🔧 Catalog & Schema Structure (Unity Catalog)
```
inv_risk_mgmt/ # Unity Catalog
├── bronze/ # Raw layer (11 tables)
│ ├── branches
│ ├── business_units
│ ├── cl_categories
│ ├── customer_types
│ ├── customers
│ ├── districts
│ ├── divisions
│ ├── financing_accounts
│ ├── financing_products
│ ├── industries
│ └── thanas
├── silver/ # Cleaned layer (11 tables)
│ ├── branches
│ ├── business_units
│ ├── cl_categories
│ ├── customer_types
│ ├── customers
│ ├── districts
│ ├── divisions
│ ├── financing_accounts
│ ├── financing_products
│ ├── industries
│ └── thanas
└── gold/ # Star Schema (7 tables)
├── dim_branch
├── dim_business_unit
├── dim_cl_category
├── dim_customer
├── dim_financing_product
├── dim_industry
└── fact_financing_account
```