# ❄️ Snowflake Credit Usage Analyzer

A comprehensive Streamlit dashboard for analyzing Snowflake credit usage and identifying optimization opportunities.

## Overview

This Streamlit application analyzes the Snowflake `QUERY_HISTORY` table from the past 24 hours to identify credit usage spikes and provide actionable recommendations for optimization.

## Features

### 📊 Comprehensive Analysis
- **Cartesian Join Detection**: Identifies queries with missing JOIN conditions or cross joins
- **Memory Spilling**: Detects queries spilling to local or remote storage
- **Warehouse Sizing**: Identifies oversized or undersized warehouses
- **Partition Pruning**: Finds queries with poor partition pruning efficiency
- **Cache Efficiency**: Highlights queries with low cache usage
- **Compilation Issues**: Detects queries with excessive compilation time

### 📈 Visualizations
- Credit usage trends over time
- Query volume and execution time patterns
- Top offenders by execution time
- Warehouse-level breakdowns
- Query type distributions

### 💡 Recommendations
Each identified issue includes specific recommendations for:
- Reducing credit consumption
- Improving query performance
- Optimizing warehouse configuration
- Better data access patterns

## Deployment Instructions

### Prerequisites
1. **Snowflake Account** with Snowsight access
2. **Permissions**: ACCOUNTADMIN role or IMPORTED PRIVILEGES on SNOWFLAKE database
3. **Anaconda Terms**: Must be accepted in your Snowflake account

### Step-by-Step Deployment

#### 1. Accept Anaconda Terms (If Not Already Done)
```
1. Sign in to Snowsight at app.snowflake.com
2. Click on your name (bottom-left corner) → Switch Role → ACCOUNTADMIN
3. Navigate to Admin → Billing & Terms
4. Find "Anaconda" section and click "Enable"
```

#### 2. Create Database and Warehouse for Streamlit
```sql
-- Run these commands in Snowsight SQL worksheet
CREATE DATABASE IF NOT EXISTS STREAMLIT_DB;
CREATE SCHEMA IF NOT EXISTS STREAMLIT_DB.CREDIT_ANALYZER;
CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH 
    WITH WAREHOUSE_SIZE='X-SMALL' 
    AUTO_SUSPEND=60 
    AUTO_RESUME=TRUE;
```

#### 3. Create the Streamlit App

1. **Navigate to Streamlit in Snowsight**
   - Sign in to Snowsight
   - Go to **Projects** → **Streamlit** (left navigation menu)
   - Click **+ Streamlit App**

2. **Configure App Settings**
   - **App name**: `Credit Usage Analyzer`
   - **Warehouse**: Select `STREAMLIT_WH` (or your preferred warehouse)
   - **App location**: 
     - Database: `STREAMLIT_DB`
     - Schema: `CREDIT_ANALYZER`
   - Click **Create**

3. **Copy the Application Code**
   - The Streamlit editor will open
   - **Delete** the default example code
   - **Copy and paste** the entire contents of `streamlit_app.py` from this repository
   - Click **Run** (top-right corner)

4. **Wait for App to Load**
   - The app will take 30-60 seconds to initialize on first run
   - You should see the dashboard with credit usage analysis

#### 4. Grant Access to Other Users (Optional)

```sql
-- Grant access to specific roles
GRANT USAGE ON DATABASE STREAMLIT_DB TO ROLE YOUR_ROLE_NAME;
GRANT USAGE ON SCHEMA STREAMLIT_DB.CREDIT_ANALYZER TO ROLE YOUR_ROLE_NAME;
GRANT USAGE ON STREAMLIT STREAMLIT_DB.CREDIT_ANALYZER.CREDIT_USAGE_ANALYZER TO ROLE YOUR_ROLE_NAME;
```

Or use the **Share** button in the Streamlit app interface.

## Required Permissions

The app requires read access to:
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`

These views are typically accessible to:
- **ACCOUNTADMIN** role
- Roles with **IMPORTED PRIVILEGES** on the SNOWFLAKE database

### Grant Permissions Example
```sql
-- Option 1: Use ACCOUNTADMIN role when viewing the app

-- Option 2: Grant specific access to other roles
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE YOUR_ROLE_NAME;
```

## Usage

### Dashboard Tabs

1. **📊 Overview**
   - Summary metrics (total queries, avg execution time, data scanned, credits used)
   - Credit usage by warehouse
   - Query distribution by type and user

2. **🔍 Issues Analysis**
   - Detailed breakdown of all detected issues:
     - Cartesian Joins
     - Memory Spilling
     - Warehouse Sizing Problems
     - Poor Partition Pruning
     - Cache Inefficiency
     - Long Compilation Times
   - Each issue includes specific recommendations

3. **⚠️ Top Offenders**
   - Top 20 most expensive queries by execution time
   - Execution time distribution histogram
   - Quick identification of performance bottlenecks

4. **📈 Trends**
   - Query volume over time (hourly)
   - Average execution time trends
   - Credit usage patterns
   - Identifies peak usage periods

## Issues Detected & Recommendations

### 🔴 Cartesian Joins
**Detection Criteria:**
- Missing ON clause in JOIN statements
- Explicit CROSS JOIN usage
- Row explosion (rows produced >> bytes scanned)

**Recommendations:**
- Add explicit JOIN conditions with ON clause
- Avoid CROSS JOINs unless absolutely necessary
- Use range join optimization or ASOF joins for time-series data
- Break complex joins into CTEs or temp tables

### 🟠 Memory Spilling
**Detection Criteria:**
- `BYTES_SPILLED_TO_LOCAL_STORAGE` > 0
- `BYTES_SPILLED_TO_REMOTE_STORAGE` > 0 (CRITICAL)

**Recommendations:**
- Increase warehouse size to provide more memory
- Optimize queries with filters to reduce intermediate data
- Use CTEs or temp tables to break down complex operations
- Consider clustering on frequently joined columns

### 🟡 Warehouse Sizing
**Detection Criteria:**
- Large warehouses with fast average query times (<5 seconds)
- Significant query queuing (QUEUED_OVERLOAD_TIME)

**Recommendations:**
- Downsize oversized warehouses (e.g., X-LARGE → MEDIUM)
- Enable multi-cluster mode for warehouses with queuing
- Separate workloads by warehouse type (ETL, BI, Ad-hoc)
- Set appropriate auto-suspend times (1-5 minutes)

### 🔵 Poor Partition Pruning
**Detection Criteria:**
- Scanning >50% of partitions when table has >100 partitions

**Recommendations:**
- Add WHERE clause filters on clustered columns
- Define clustering keys on frequently filtered columns
- Use date/timestamp filters to leverage micro-partition pruning
- Review table design and clustering strategy

### ⚪ Cache Inefficiency
**Detection Criteria:**
- Low cache percentage (<20%) on queries >10 seconds

**Recommendations:**
- Reduce auto-suspend time to keep warehouse warm
- Enable result caching for repeated queries
- Schedule similar queries on the same warehouse
- Consider dedicating warehouses to specific workload patterns

### ⚪ Long Compilation Time
**Detection Criteria:**
- Compilation time >30% of total elapsed time
- Compilation time >5 seconds

**Recommendations:**
- Simplify complex queries
- Reduce number of CTEs
- Avoid dynamic SQL where possible
- Break large queries into smaller, modular pieces
- Use persistent derived tables instead of views in complex joins

## Best Practices

### Credit Optimization Checklist
- ✅ Set auto-suspend to 1-5 minutes for all warehouses
- ✅ Start with X-Small/Small and scale based on performance metrics
- ✅ Create separate warehouses for ETL, BI, and Analytics workloads
- ✅ Use clustering keys on frequently filtered columns
- ✅ Avoid `SELECT *` - scan only necessary columns
- ✅ Leverage result caching for repeated queries
- ✅ Monitor and review query patterns weekly
- ✅ Set statement timeouts to prevent runaway queries

### Warehouse Size Guide
| Size | Credits/Hour | Recommended Use Case |
|------|--------------|---------------------|
| X-Small | 1 | Light BI, simple reporting |
| Small | 2 | Standard BI dashboards |
| Medium | 4 | ETL, moderate complexity |
| Large | 8 | Heavy ETL, complex analytics |
| X-Large+ | 16+ | Large-scale transformations, ML |

## Troubleshooting

### "No query history data available"
**Cause**: Insufficient permissions or no queries in last 24 hours

**Solution**:
```sql
-- Check if you can access QUERY_HISTORY
SELECT COUNT(*) 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- If access denied, switch to ACCOUNTADMIN or grant privileges
USE ROLE ACCOUNTADMIN;
```

### "Error loading query history"
**Cause**: Latency in ACCOUNT_USAGE views (up to 45 minutes)

**Solution**: Wait a few minutes and refresh, or adjust the time window in the query

### App Runs Slowly
**Cause**: Processing large query history

**Solutions**:
- Reduce time window (change -24 hours to -12 hours in the query)
- Increase Streamlit warehouse size
- Add additional filters to the base query

## Customization

### Adjust Time Window
In `streamlit_app.py`, modify the time window in `load_query_history()`:
```python
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())  # Change -24 to your preferred hours
```

### Add Custom Issue Detection
Create new analysis functions following this pattern:
```python
def analyze_custom_issue(df):
    issues = []
    for idx, row in df.iterrows():
        # Your detection logic
        if condition:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'SEVERITY': 'HIGH',
                'ISSUE': 'Issue Name',
                'RECOMMENDATION': 'Your recommendation'
            })
    return pd.DataFrame(issues)
```

### Modify Thresholds
Adjust detection thresholds in the analysis functions:
- Cartesian join row threshold: `rows_produced > 1000000`
- Spilling detection: `local_spill > 0`
- Warehouse sizing avg time: `avg_exec < 5`
- Partition scan percentage: `scan_percentage > 50`
- Cache efficiency: `cache_percentage < 20`
- Compilation time: `compilation_pct > 30`

## Support & Feedback

For issues, questions, or feature requests:
1. Check Snowflake's official documentation
2. Review the Query Profile for individual queries
3. Contact your Snowflake account team for credit optimization consultations

## Version History

- **v1.0** (December 2025)
  - Initial release
  - Supports analysis of QUERY_HISTORY and WAREHOUSE_METERING_HISTORY
  - Detects 6 major issue categories
  - Provides actionable recommendations
  - Interactive visualizations with Plotly

## License

This application is provided as-is for use within Snowflake environments.

---

**Built with ❄️ by your Data Engineering team**
