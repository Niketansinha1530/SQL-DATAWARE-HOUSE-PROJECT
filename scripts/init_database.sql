-- ================================================================
-- 🎯 Project: Data Warehouse Initialization Script
-- 📝 Description: Creates the main database and essential schemas
-- 👨‍💻 Author: Niketan Sinha
-- 📅 Created On: 2025-08-04
-- ================================================================

-- 🚩 Step 1: Create the main database (run this from postgres db)
-- NOTE: PostgreSQL does not allow database creation from within itself
-- Run this part only from another session connected to a different DB (e.g., postgres)

CREATE DATABASE DataWarehouse;

-- After running the above line, connect to the new database:
-- \c DataWarehouse

-- 🚩 Step 2: Create schemas inside the DataWarehouse database

-- ✅ Bronze Layer – Raw ingested data
CREATE SCHEMA IF NOT EXISTS bronze;

-- ✅ Silver Layer – Cleaned and processed data
CREATE SCHEMA IF NOT EXISTS silver;

-- ✅ Gold Layer – Aggregated, business-ready data
CREATE SCHEMA IF NOT EXISTS gold;

