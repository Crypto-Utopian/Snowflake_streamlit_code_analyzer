# Snowflake Credit Usage Analyzer - Snowsight Dashboard

## Project Overview

This is a **Snowsight-only Streamlit application** designed to run exclusively within Snowflake's Snowsight web interface. It analyzes query performance and credit usage to help Data Engineering teams optimize Snowflake costs.

## Important: Deployment Model

⚠️ **This application CANNOT run locally or in Replit.**

### Why Snowsight-Only?

This app requires:
1. **Snowflake Session Context**: Uses `get_active_session()` which only exists in Snowflake's managed Streamlit environment
2. **ACCOUNT_USAGE Access**: Queries `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` and `WAREHOUSE_METERING_HISTORY` views
3. **Snowflake Privileges**: Requires ACCOUNTADMIN role or IMPORTED PRIVILEGES on SNOWFLAKE database
4. **Managed Environment**: Runs on Snowflake's infrastructure with direct access to metadata

### How to Deploy

Follow the instructions in `DEPLOYMENT_GUIDE.md`:

1. Log into Snowsight (app.snowflake.com)
2. Navigate to Projects → Streamlit
3. Create new Streamlit app
4. Copy `streamlit_app.py` contents into the editor
5. Run the app

## Features

### Analysis Capabilities (15+ Detectors)

**SQL Anti-Patterns:**
- **SELECT * Detection**: Identifies queries selecting all columns unnecessarily
- **Cartesian Join Detection**: Missing JOIN conditions, CROSS JOINs, OR in JOINs
- **UNION vs UNION ALL**: Detects unnecessary duplicate elimination
- **Functions on Filter Columns**: YEAR(), DATE(), UPPER() etc. disabling pruning

**Performance Issues:**
- **Memory Spilling Analysis**: Local and remote spilling (warehouse undersizing)
- **Poor Partition Pruning**: High partition scan percentages
- **Warehouse Sizing Issues**: Oversized/undersized warehouses, queuing
- **Cache Utilization**: Low cache hit rates on expensive queries
- **Compilation Overhead**: Excessive query compilation times
- **Repeated Expensive Queries**: Same costly queries running repeatedly
- **Full Table Scans**: Large unfiltered scans
- **Query Retries**: OOM and failure recovery issues
- **Cloud Services Credits**: High metadata operation costs

### Visualizations
- Credit usage trends
- Query volume patterns
- Top expensive queries
- Warehouse-level breakdowns
- Time-series analysis

### Recommendations
Each issue includes specific, actionable recommendations for:
- Reducing credit consumption
- Improving query performance
- Optimizing warehouse configuration

## Project Structure

```
.
├── streamlit_app.py           # Main Streamlit application (deploy to Snowsight)
├── README.md                  # Comprehensive documentation
├── DEPLOYMENT_GUIDE.md        # Quick deployment steps
├── USER_GUIDE.md              # Dashboard usage guide with UI walkthrough
├── SNOWSIGHT_SETUP.sql        # SQL setup scripts for permissions
└── replit.md                  # This file - project information
```

## Technical Details

### Data Sources
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`: Past 365 days, 45-min latency
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`: Credit usage data

### Analysis Logic

1. **Cartesian Joins**: Detects missing ON clauses, CROSS JOINs, row explosion
2. **Spilling**: Checks BYTES_SPILLED_TO_LOCAL/REMOTE_STORAGE
3. **Warehouse Sizing**: Compares avg execution time vs warehouse size
4. **Pruning**: Calculates partition scan percentage
5. **Cache**: Analyzes PERCENTAGE_SCANNED_FROM_CACHE
6. **Compilation**: Measures compilation time as % of total time

### Issue Severity Levels
- **CRITICAL**: Immediate action required (e.g., remote spilling, missing JOIN)
- **HIGH**: Significant impact (e.g., excessive queuing)
- **MEDIUM**: Moderate impact (e.g., oversized warehouse)
- **LOW**: Minor optimization opportunity

## User Preferences

- **Target Users**: Data Engineers, Analytics Engineers, Snowflake Administrators
- **Primary Goal**: Identify and fix credit usage spikes from inefficient queries
- **Analysis Window**: Last 24 hours (configurable)
- **Deployment**: Snowsight-only (not local, not external hosting)

## Dependencies

All dependencies are managed by Snowflake's Anaconda integration:
- `streamlit` (provided by Snowflake)
- `snowflake-snowpark-python` (provided by Snowflake)
- `pandas` (provided by Snowflake)
- `plotly` (may need to be added via Snowflake's package manager)

## Customization

Users can customize:
- Time window (default: 24 hours)
- Detection thresholds for each issue type
- Severity classifications
- Additional analysis functions

See README.md for customization examples.

## Monitoring Recommendations

After deployment, review the dashboard:
- **Daily**: Check "Issues Analysis" tab for new problems
- **Weekly**: Review "Trends" tab for patterns
- **Monthly**: Analyze "Top Offenders" for recurring issues

## Best Practices Implemented

- ✅ Uses Snowflake's managed Streamlit environment
- ✅ Leverages ACCOUNT_USAGE for comprehensive history
- ✅ Caches data for 5 minutes to reduce query load
- ✅ Provides actionable, specific recommendations
- ✅ Color-coded severity levels for quick triage
- ✅ Interactive visualizations with Plotly
- ✅ Handles missing permissions gracefully

## Limitations

- **Data Latency**: ACCOUNT_USAGE has up to 45-minute latency
- **Historical Data**: Only analyzes past 24 hours by default
- **Permissions**: Requires elevated privileges (ACCOUNTADMIN)
- **Deployment**: Must be deployed in Snowsight (cannot run externally)

## Future Enhancements

Potential additions:
- Email/Slack alerts for critical issues
- Historical trend comparison (week-over-week)
- Query recommendation engine
- Integration with query optimization tools
- Automated warehouse resizing suggestions
- Cost forecasting based on patterns

## Support

For deployment issues:
1. Review DEPLOYMENT_GUIDE.md
2. Check Snowflake permissions
3. Verify ACCOUNT_USAGE access
4. Contact Snowflake support for account-specific issues

---

**Last Updated**: December 2, 2025  
**Version**: 1.0  
**Platform**: Snowflake Snowsight
