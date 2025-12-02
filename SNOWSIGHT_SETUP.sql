-- ============================================================================
-- Snowflake Credit Usage Analyzer - Setup Script
-- ============================================================================
-- This script prepares your Snowflake environment for the Streamlit dashboard
-- Run these commands in Snowsight SQL Worksheet
-- ============================================================================

-- Step 1: Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create database and schema for Streamlit app
CREATE DATABASE IF NOT EXISTS STREAMLIT_DB
    COMMENT = 'Database for Streamlit applications';

CREATE SCHEMA IF NOT EXISTS STREAMLIT_DB.CREDIT_ANALYZER
    COMMENT = 'Schema for Credit Usage Analyzer app';

-- Step 3: Create warehouse for Streamlit app
CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH 
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Streamlit applications';

-- Step 4: Verify access to required ACCOUNT_USAGE views
-- These queries should return data without errors

-- Check QUERY_HISTORY access
SELECT 
    COUNT(*) AS query_count,
    MIN(START_TIME) AS earliest_query,
    MAX(START_TIME) AS latest_query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- Check WAREHOUSE_METERING_HISTORY access
SELECT 
    COUNT(*) AS record_count,
    SUM(CREDITS_USED) AS total_credits_24h
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- Step 5: (Optional) Create role for Streamlit app users
CREATE ROLE IF NOT EXISTS STREAMLIT_USER_ROLE
    COMMENT = 'Role for users accessing Streamlit dashboards';

-- Step 6: (Optional) Grant necessary privileges to other roles
-- Uncomment and modify as needed for your organization

-- Grant database and schema access
-- GRANT USAGE ON DATABASE STREAMLIT_DB TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON SCHEMA STREAMLIT_DB.CREDIT_ANALYZER TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON WAREHOUSE STREAMLIT_WH TO ROLE DATA_ENGINEER;

-- Grant access to ACCOUNT_USAGE (required for the dashboard)
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE DATA_ENGINEER;

-- Step 7: Verify Anaconda terms are accepted
-- This CANNOT be done via SQL - must be done in UI:
-- 1. Go to Admin → Billing & Terms
-- 2. Find "Anaconda" section
-- 3. Click "Enable" if not already enabled

-- ============================================================================
-- Setup Complete!
-- ============================================================================
-- Next Steps:
-- 1. Ensure Anaconda terms are accepted (see Step 7)
-- 2. Go to Projects → Streamlit in Snowsight
-- 3. Click "+ Streamlit App"
-- 4. Configure:
--    - App name: Credit Usage Analyzer
--    - Warehouse: STREAMLIT_WH
--    - Location: STREAMLIT_DB.CREDIT_ANALYZER
-- 5. Copy contents of streamlit_app.py into the editor
-- 6. Click Run
-- ============================================================================

-- Verify setup
SELECT 'Setup verification successful! You can now create the Streamlit app.' AS status;
