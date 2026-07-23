## 📊 Architecture
```
CSV Files → Bronze (Raw) → Silver (Cleaned) → Gold (Star Schema)
Raw Data Cleaned & Facts │ Ingestion Enriched Dimensions │ Data Tables
```
## 📂 Notebook Structure
```
The project is organized into 2 main folders in Databricks:
📁 data_processing/ # ETL Processing & Metadata
├── data_loading.py # Load CSV → Bronze (Raw Data)
├── data_transformation.py # Bronze → Silver (Clean & Enrich)
├── star_schema.py # Silver → Gold (Star Schema)
└── metadata.py # Schemas, primary keys, business logic

📁 schema_definition/ # DDL Scripts
├── bronze_tables.sql # Create Bronze layer (11 tables)
├── silver_tables.sql # Create Silver layer (11 tables)
├── gold_tables.sql # Create Gold layer (7 tables)
├── drop_silver_tables.sql # Utility: Drop all Silver tables
└── additional_scripts.sql # Additional DDL scripts