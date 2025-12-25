# Snowflake Credit Usage Analyzer - User Guide

## Overview

The Snowflake Credit Usage Analyzer is a Streamlit dashboard that helps Data Engineering teams identify and fix credit-wasting queries. It analyzes your Snowflake QUERY_HISTORY to detect optimization opportunities and provides actionable recommendations.

---

## Dashboard Layout

### Main Interface

```
+------------------+------------------------------------------------+
|                  |  Snowflake Credit Usage Analyzer               |
|    SIDEBAR       |  ============================================  |
|                  |                                                |
|  Time Window     |  METRICS ROW                                   |
|  [====24h====]   |  +--------+ +--------+ +--------+ +--------+   |
|                  |  |Queries | |Issues  | |Critical| |Credits |   |
|  Users           |  | 1,234  | |  45    | |   3    | | 12.50  |   |
|  [Select...]     |  +--------+ +--------+ +--------+ +--------+   |
|                  |                                                |
|  Roles           |  CATEGORY BUTTONS                              |
|  [Select...]     |  [SQL Anti-Patterns] [Performance] [Anomalies] |
|                  |                                                |
|  Warehouses      |  DETAIL VIEW                                   |
|  [Select...]     |  (Expands based on selected category)          |
|                  |                                                |
|  Databases       |                                                |
|  [Select...]     |                                                |
+------------------+------------------------------------------------+
```

---

## Using the Sidebar Filters

### Time Window
Adjust the slider to analyze queries from the past 1-168 hours (default: 24 hours).

### Filter by User
Select specific users to focus on their query patterns. Leave empty to include all users.

### Filter by Role
Narrow analysis to queries run under specific roles (e.g., ANALYST, ETL_ROLE).

### Filter by Warehouse
Focus on specific warehouses to identify per-warehouse optimization opportunities.

### Filter by Database
Limit analysis to queries targeting specific databases.

**Tip:** The sidebar shows both "Raw data" and "Filtered" query counts so you can see how your filters affect the scope of analysis.

---

## Issue Categories

### SQL Anti-Patterns (Red)

| Issue | What It Means | How to Fix |
|-------|---------------|------------|
| SELECT * | Queries selecting all columns | Specify only needed columns |
| Cartesian Joins | Missing JOIN conditions | Add ON/USING clauses |
| UNION vs UNION ALL | Unnecessary duplicate elimination | Use UNION ALL when duplicates are acceptable |
| Functions on Filters | YEAR(), DATE() in WHERE clause | Use date ranges instead |

### Performance Issues (Orange)

| Issue | What It Means | How to Fix |
|-------|---------------|------------|
| Memory Spilling | Query exceeds warehouse memory | Upsize warehouse or optimize query |
| Poor Pruning | Scanning too many partitions | Add clustering keys |
| Warehouse Sizing | Over/undersized warehouses | Right-size based on workload |
| Slow Compilation | Complex query structure | Simplify or use temp tables |
| Low Cache | Not utilizing result cache | Increase auto-suspend time |

### Operational Issues (Blue)

| Issue | What It Means | How to Fix |
|-------|---------------|------------|
| Repeated Expensive | Same query runs multiple times | Create materialized view |
| Full Table Scans | Large scans without filters | Add WHERE clause or LIMIT |

### Anomalies (Purple)

| Anomaly | What It Detects | Action |
|---------|-----------------|--------|
| Redundant Executions | Same query 3+ times in 15 min | Review job scheduling |
| Runtime Spikes | Query 3x slower than usual | Investigate data skew |
| Off-Hours Queries | Queries midnight-5AM | Verify intentional scheduling |

---

## Workflow: Finding and Fixing Issues

### Step 1: Start Broad
Open the dashboard with default settings to see overall issue counts across all users and warehouses.

### Step 2: Identify Hot Spots
Look at the metrics row:
- High "Critical Issues" count = immediate attention needed
- High "Anomalies" count = unusual patterns to investigate

### Step 3: Drill Down
Click a category button to see detailed issues:
- Each expander shows the problem, a fix example, and affected queries
- Use QUERY_ID to find the exact query in Snowflake History

### Step 4: Apply Filters
Use sidebar filters to focus on:
- A specific user reporting slow queries
- A warehouse with high credit usage
- A database undergoing optimization

### Step 5: Implement Fixes
Copy the SQL recommendations provided and apply them to your queries or data models.

---

## Example Use Cases

### Use Case 1: Credit Spike Investigation
1. Set Time Window to cover the spike period
2. Click "Trends" to see credit usage by warehouse
3. Filter by the highest-credit warehouse
4. Check "SQL Anti-Patterns" for root causes

### Use Case 2: User Query Audit
1. Filter by specific USER_NAME
2. Review all categories for that user's issues
3. Share specific QUERY_IDs and recommendations

### Use Case 3: Warehouse Right-Sizing
1. Click "Performance" category
2. Expand "Warehouse Sizing" section
3. Note oversized (high cost, low utilization) or queuing (undersized) issues

### Use Case 4: Anomaly Detection
1. Click "Anomalies" button
2. Check "Redundant Executions" for scheduling issues
3. Review "Runtime Spikes" for unexpected slowdowns
4. Verify "Off-Hours Queries" are intentional

---

## Severity Levels

| Level | Color | Meaning |
|-------|-------|---------|
| CRITICAL | Red | Immediate action required (e.g., remote spilling, missing JOIN) |
| HIGH | Orange | Significant impact on credits or performance |
| MEDIUM | Yellow | Moderate optimization opportunity |
| LOW | Blue | Minor improvement possible |

---

## Data Sources

The dashboard queries:
- `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` - Query execution details
- `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` - Credit consumption

**Note:** ACCOUNT_USAGE views have up to 45-minute data latency.

---

## Permissions Required

```sql
-- Option 1: Use ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Option 2: Grant imported privileges to your role
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE your_role;
```

---

## Tips for Best Results

1. **Start with 24 hours** - Good balance of data volume and relevance
2. **Filter incrementally** - Add one filter at a time to understand impact
3. **Prioritize CRITICAL** - Fix these first for biggest credit savings
4. **Check Anomalies regularly** - Catch scheduling issues early
5. **Use Trends** - Identify patterns before they become problems

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No data showing | Check permissions, wait for ACCOUNT_USAGE latency |
| All zeros | No queries in time window, extend Time Window |
| Missing warehouses in filter | No queries from that warehouse in time window |

---

## Support

For issues with:
- **Dashboard deployment** - See DEPLOYMENT_GUIDE.md
- **Snowflake permissions** - Contact your Snowflake administrator
- **Feature requests** - Modify streamlit_app.py as needed

---

*Last Updated: December 2025*
