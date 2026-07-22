# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG inv_risk_mgmt;
# MAGIC USE SCHEMA silver;
# MAGIC
# MAGIC drop table if exists DIVISIONS;
# MAGIC drop table if exists DISTRICTS;
# MAGIC drop table if exists THANAS;
# MAGIC drop table if exists branches;
# MAGIC drop table if exists CL_CATEGORIES;
# MAGIC drop table if exists BUSINESS_UNITS;
# MAGIC drop table if exists CUSTOMER_TYPES;
# MAGIC drop table if exists CUSTOMERS;
# MAGIC drop table if exists INDUSTRIES;
# MAGIC drop table if exists FINANCING_PRODUCTS;
# MAGIC drop table if exists FINANCING_ACCOUNTS;
# MAGIC