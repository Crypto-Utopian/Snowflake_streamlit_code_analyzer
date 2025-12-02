import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import re

st.set_page_config(layout="wide", page_icon="❄️", page_title="Snowflake Credit Usage Analyzer")

session = get_active_session()

st.title("❄️ Snowflake Credit Usage Analyzer")
st.markdown("**Advanced SQL Analysis & Credit Optimization Dashboard**")
st.markdown("---")

@st.cache_data(ttl=300)
def load_query_history():
    """Load query history from the past 24 hours"""
    query = """
    SELECT 
        QUERY_ID,
        QUERY_TEXT,
        QUERY_TYPE,
        QUERY_PARAMETERIZED_HASH,
        USER_NAME,
        ROLE_NAME,
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        DATABASE_NAME,
        SCHEMA_NAME,
        START_TIME,
        END_TIME,
        TOTAL_ELAPSED_TIME,
        EXECUTION_TIME,
        COMPILATION_TIME,
        QUEUED_PROVISIONING_TIME,
        QUEUED_OVERLOAD_TIME,
        TRANSACTION_BLOCKED_TIME,
        BYTES_SCANNED,
        BYTES_WRITTEN,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        PARTITIONS_SCANNED,
        PARTITIONS_TOTAL,
        PERCENTAGE_SCANNED_FROM_CACHE,
        ROWS_PRODUCED,
        ROWS_INSERTED,
        ROWS_UPDATED,
        ROWS_DELETED,
        EXECUTION_STATUS,
        ERROR_CODE,
        ERROR_MESSAGE,
        CREDITS_USED_CLOUD_SERVICES,
        QUERY_RETRY_TIME,
        QUERY_RETRY_CAUSE
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'SUCCESS'
        AND QUERY_TYPE NOT IN ('SHOW', 'DESCRIBE', 'USE', 'GRANT', 'REVOKE')
        AND TOTAL_ELAPSED_TIME > 1000
    ORDER BY START_TIME DESC
    """
    
    try:
        df = session.sql(query).to_pandas()
        df['TOTAL_ELAPSED_TIME_SEC'] = df['TOTAL_ELAPSED_TIME'] / 1000
        df['EXECUTION_TIME_SEC'] = df['EXECUTION_TIME'] / 1000
        df['COMPILATION_TIME_SEC'] = df['COMPILATION_TIME'] / 1000 if 'COMPILATION_TIME' in df.columns else 0
        return df
    except Exception as e:
        st.error(f"Error loading query history: {str(e)}")
        st.info("Note: This app requires access to SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY. Please ensure you have ACCOUNTADMIN role or appropriate privileges.")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_warehouse_metering():
    """Load warehouse credit usage"""
    query = """
    SELECT 
        WAREHOUSE_NAME,
        START_TIME,
        END_TIME,
        CREDITS_USED,
        CREDITS_USED_COMPUTE,
        CREDITS_USED_CLOUD_SERVICES
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    ORDER BY START_TIME DESC
    """
    
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.warning(f"Could not load warehouse metering data: {str(e)}")
        return pd.DataFrame()

def analyze_select_star(df):
    """Detect SELECT * anti-pattern - the most common issue"""
    issues = []
    
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        
        select_star_patterns = [
            r'SELECT\s+\*\s+FROM',
            r'SELECT\s+[A-Z_]+\.\*',
        ]
        
        has_select_star = any(re.search(pattern, query_text) for pattern in select_star_patterns)
        
        if has_select_star:
            bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
            severity = 'HIGH' if bytes_scanned > 1073741824 else 'MEDIUM'  # > 1GB
            
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                'SEVERITY': severity,
                'ISSUE': 'SELECT * Usage',
                'PROBLEM': 'Forces Snowflake to process ALL columns, increasing data transfer and credits.',
                'RECOMMENDATION': '''Replace SELECT * with specific columns:
```sql
-- Instead of:
SELECT * FROM my_table

-- Use:
SELECT column1, column2, column3 FROM my_table
```
This reduces I/O by only scanning required columns.'''
            })
    
    return pd.DataFrame(issues)

def analyze_cartesian_joins(df):
    """Identify potential cartesian joins and missing join conditions"""
    issues = []
    
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        
        has_join = 'JOIN' in query_text
        has_on_or_using = ' ON ' in query_text or 'USING' in query_text
        has_comma_join = re.search(r'FROM\s+\w+\s*,\s*\w+', query_text) and 'WHERE' not in query_text
        has_cross_join = 'CROSS JOIN' in query_text
        has_or_in_join = re.search(r'JOIN.*ON.*\sOR\s', query_text, re.DOTALL)
        
        rows_produced = row['ROWS_PRODUCED'] if pd.notna(row['ROWS_PRODUCED']) else 0
        bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
        execution_time = row['EXECUTION_TIME_SEC']
        
        high_row_explosion = (
            rows_produced > 10000000 and 
            bytes_scanned > 0 and  
            execution_time > 60 and
            (rows_produced / max(bytes_scanned, 1)) > 100
        )
        
        missing_join_condition = (has_join and not has_on_or_using) or has_comma_join
        
        if missing_join_condition or has_cross_join or high_row_explosion or has_or_in_join:
            severity = 'CRITICAL' if (missing_join_condition or has_cross_join) else 'HIGH'
            reason = []
            recommendation = ""
            
            if missing_join_condition:
                reason.append("Missing ON/USING clause in JOIN")
                recommendation = '''Add explicit JOIN conditions:
```sql
-- Instead of:
SELECT * FROM orders, customers

-- Use:
SELECT o.*, c.name 
FROM orders o
JOIN customers c ON o.customer_id = c.id
```'''
            elif has_cross_join:
                reason.append("Explicit CROSS JOIN detected")
                recommendation = '''CROSS JOINs create Cartesian products. If intentional, add a comment. Otherwise:
```sql
-- Replace CROSS JOIN with proper JOIN:
SELECT * FROM t1
JOIN t2 ON t1.key = t2.key
```'''
            elif has_or_in_join:
                reason.append("OR condition in JOIN clause")
                recommendation = '''OR in JOIN clauses is inefficient. Use UNION instead:
```sql
-- Instead of:
FROM t1 JOIN t2 ON t1.id = t2.id OR t1.alt_id = t2.alt_id

-- Use:
FROM t1 JOIN t2 ON t1.id = t2.id
UNION ALL
FROM t1 JOIN t2 ON t1.alt_id = t2.alt_id
```'''
            elif high_row_explosion:
                reason.append(f"Row explosion: {rows_produced:,} rows produced")
                recommendation = '''Check for duplicate keys causing many-to-many join. Solutions:
1. Add DISTINCT to input data
2. Add additional join conditions
3. Verify key uniqueness with: SELECT key, COUNT(*) FROM table GROUP BY key HAVING COUNT(*) > 1'''
            
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': execution_time,
                'ROWS_PRODUCED': rows_produced,
                'SEVERITY': severity,
                'ISSUE': 'Cartesian Join / Join Issue',
                'PROBLEM': ' | '.join(reason),
                'RECOMMENDATION': recommendation
            })
    
    return pd.DataFrame(issues)

def analyze_union_vs_union_all(df):
    """Detect UNION when UNION ALL might suffice"""
    issues = []
    
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        
        has_union = bool(re.search(r'\bUNION\b(?!\s+ALL)', query_text))
        
        if has_union:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'SEVERITY': 'LOW',
                'ISSUE': 'UNION Instead of UNION ALL',
                'PROBLEM': 'UNION performs costly duplicate elimination. UNION ALL is faster if duplicates are acceptable.',
                'RECOMMENDATION': '''If duplicates are acceptable or impossible, use UNION ALL:
```sql
-- Instead of (slow - removes duplicates):
SELECT col FROM table1
UNION
SELECT col FROM table2

-- Use (fast - keeps all rows):
SELECT col FROM table1
UNION ALL
SELECT col FROM table2
```
UNION ALL can be 2-3x faster for large datasets.'''
            })
    
    return pd.DataFrame(issues)

def analyze_function_on_filter(df):
    """Detect functions on filter columns that prevent partition pruning"""
    issues = []
    
    function_patterns = [
        (r'\bYEAR\s*\(\s*\w+', 'YEAR()'),
        (r'\bMONTH\s*\(\s*\w+', 'MONTH()'),
        (r'\bDATE\s*\(\s*\w+', 'DATE()'),
        (r'\bTO_DATE\s*\(\s*\w+', 'TO_DATE()'),
        (r'\bDATE_TRUNC\s*\(\s*[\'"]?\w+[\'"]?\s*,\s*\w+', 'DATE_TRUNC()'),
        (r'\bUPPER\s*\(\s*\w+', 'UPPER()'),
        (r'\bLOWER\s*\(\s*\w+', 'LOWER()'),
        (r'\bTRIM\s*\(\s*\w+', 'TRIM()'),
        (r'\bSUBSTR\s*\(\s*\w+', 'SUBSTR()'),
    ]
    
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        
        if 'WHERE' not in query_text:
            continue
            
        where_clause = query_text.split('WHERE', 1)[-1]
        where_clause = where_clause.split('GROUP BY')[0] if 'GROUP BY' in where_clause else where_clause
        where_clause = where_clause.split('ORDER BY')[0] if 'ORDER BY' in where_clause else where_clause
        where_clause = where_clause.split('LIMIT')[0] if 'LIMIT' in where_clause else where_clause
        
        detected_functions = []
        
        for pattern, func_name in function_patterns:
            if re.search(pattern, where_clause, re.IGNORECASE):
                detected_functions.append(func_name)
        
        if detected_functions:
            partitions_scanned = row['PARTITIONS_SCANNED'] if pd.notna(row['PARTITIONS_SCANNED']) else 0
            partitions_total = row['PARTITIONS_TOTAL'] if pd.notna(row['PARTITIONS_TOTAL']) else 0
            
            severity = 'HIGH' if partitions_total > 100 else 'MEDIUM'
            
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'FUNCTIONS_DETECTED': ', '.join(detected_functions),
                'PARTITIONS_SCANNED': partitions_scanned,
                'SEVERITY': severity,
                'ISSUE': 'Function on Filter Column',
                'PROBLEM': f'Functions {", ".join(detected_functions)} on filter columns DISABLE partition pruning.',
                'RECOMMENDATION': '''Rewrite filters to avoid functions on columns:
```sql
-- Instead of (disables pruning):
WHERE YEAR(order_date) = 2024

-- Use (enables pruning):
WHERE order_date >= '2024-01-01' 
  AND order_date < '2025-01-01'

-- Instead of:
WHERE DATE(created_at) = '2024-06-15'

-- Use:
WHERE created_at >= '2024-06-15' 
  AND created_at < '2024-06-16'
```'''
            })
    
    return pd.DataFrame(issues)

def analyze_spilling(df):
    """Identify queries with memory spilling - indicates warehouse undersizing"""
    issues = []
    
    for idx, row in df.iterrows():
        local_spill = row['BYTES_SPILLED_TO_LOCAL_STORAGE'] if pd.notna(row['BYTES_SPILLED_TO_LOCAL_STORAGE']) else 0
        remote_spill = row['BYTES_SPILLED_TO_REMOTE_STORAGE'] if pd.notna(row['BYTES_SPILLED_TO_REMOTE_STORAGE']) else 0
        
        if local_spill > 0 or remote_spill > 0:
            severity = 'CRITICAL' if remote_spill > 0 else 'HIGH'
            total_spill_gb = (local_spill + remote_spill) / (1024**3)
            
            size_recommendation = {
                'X-SMALL': 'SMALL or MEDIUM',
                'SMALL': 'MEDIUM or LARGE',
                'MEDIUM': 'LARGE',
                'LARGE': 'X-LARGE',
                'X-LARGE': '2X-LARGE',
            }
            current_size = row['WAREHOUSE_SIZE'] if pd.notna(row['WAREHOUSE_SIZE']) else 'UNKNOWN'
            suggested_size = size_recommendation.get(current_size, 'larger size')
            
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'WAREHOUSE_SIZE': current_size,
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'LOCAL_SPILL_GB': round(local_spill / (1024**3), 2),
                'REMOTE_SPILL_GB': round(remote_spill / (1024**3), 2),
                'SEVERITY': severity,
                'ISSUE': 'Memory Spilling',
                'PROBLEM': f'Query spilled {total_spill_gb:.2f} GB to {"REMOTE storage (worst case)" if remote_spill > 0 else "local SSD"}. Warehouse memory insufficient.',
                'RECOMMENDATION': f'''Upgrade warehouse from {current_size} to {suggested_size}, OR optimize query:
```sql
-- Break into smaller operations:
CREATE TEMP TABLE step1 AS
SELECT * FROM large_table WHERE date_filter = '2024-01-01';

-- Then join with smaller dataset
SELECT * FROM step1 JOIN other_table ON ...

-- Or add more filters to reduce intermediate data:
WITH filtered_data AS (
    SELECT * FROM large_table 
    WHERE status = 'ACTIVE' AND date > '2024-01-01'
)
SELECT ... FROM filtered_data ...
```'''
            })
    
    return pd.DataFrame(issues)

def analyze_poor_pruning(df):
    """Identify queries with poor partition pruning"""
    issues = []
    
    for idx, row in df.iterrows():
        partitions_scanned = row['PARTITIONS_SCANNED'] if pd.notna(row['PARTITIONS_SCANNED']) else 0
        partitions_total = row['PARTITIONS_TOTAL'] if pd.notna(row['PARTITIONS_TOTAL']) else 0
        bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
        
        if partitions_total > 50:
            scan_percentage = (partitions_scanned / partitions_total) * 100
            
            if scan_percentage > 50:
                severity = 'HIGH' if scan_percentage > 80 else 'MEDIUM'
                
                issues.append({
                    'QUERY_ID': row['QUERY_ID'],
                    'USER_NAME': row['USER_NAME'],
                    'WAREHOUSE': row['WAREHOUSE_NAME'],
                    'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                    'PARTITIONS_SCANNED': partitions_scanned,
                    'PARTITIONS_TOTAL': partitions_total,
                    'SCAN_PERCENTAGE': round(scan_percentage, 1),
                    'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                    'SEVERITY': severity,
                    'ISSUE': 'Poor Partition Pruning',
                    'PROBLEM': f'Scanning {scan_percentage:.0f}% of partitions ({partitions_scanned:,}/{partitions_total:,}). Missing effective filters.',
                    'RECOMMENDATION': '''Add filters on clustered columns or define clustering keys:
```sql
-- Check current clustering:
SELECT SYSTEM$CLUSTERING_INFORMATION('database.schema.table');

-- Add clustering key on frequently filtered columns:
ALTER TABLE my_table CLUSTER BY (date_column, region);

-- Always filter on clustered columns:
SELECT * FROM my_table 
WHERE date_column >= '2024-01-01'  -- Enables pruning
  AND region = 'US'
```'''
                })
    
    return pd.DataFrame(issues)

def analyze_warehouse_sizing(df):
    """Identify inefficient warehouse usage"""
    issues = []
    
    grouped = df.groupby(['WAREHOUSE_NAME', 'WAREHOUSE_SIZE']).agg({
        'EXECUTION_TIME_SEC': ['mean', 'max', 'count'],
        'QUEUED_OVERLOAD_TIME': 'sum',
        'QUEUED_PROVISIONING_TIME': 'sum'
    }).reset_index()
    
    for idx, row in grouped.iterrows():
        warehouse = row['WAREHOUSE_NAME']
        size = row['WAREHOUSE_SIZE']
        avg_exec = row[('EXECUTION_TIME_SEC', 'mean')]
        query_count = row[('EXECUTION_TIME_SEC', 'count')]
        queued_overload = row[('QUEUED_OVERLOAD_TIME', 'sum')]
        queued_provision = row[('QUEUED_PROVISIONING_TIME', 'sum')]
        
        if avg_exec < 3 and size in ['LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE']:
            credits_per_hour = {'LARGE': 8, 'X-LARGE': 16, '2X-LARGE': 32, '3X-LARGE': 64, '4X-LARGE': 128}
            current_credits = credits_per_hour.get(size, 8)
            
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'AVG_EXEC_TIME_SEC': round(avg_exec, 2),
                'QUERY_COUNT': query_count,
                'CREDITS_PER_HOUR': current_credits,
                'SEVERITY': 'MEDIUM',
                'ISSUE': 'Oversized Warehouse',
                'PROBLEM': f'Queries average only {avg_exec:.1f}s but warehouse uses {current_credits} credits/hour.',
                'RECOMMENDATION': f'''Downsize warehouse to save credits:
```sql
ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = 'SMALL';
-- or
ALTER WAREHOUSE {warehouse} SET WAREHOUSE_SIZE = 'MEDIUM';
```
A SMALL warehouse (2 credits/hr) is likely sufficient for {avg_exec:.1f}s average queries.'''
            })
        
        if queued_overload > 60000:
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'QUEUED_TIME_SEC': round(queued_overload / 1000, 2),
                'QUERY_COUNT': query_count,
                'SEVERITY': 'HIGH',
                'ISSUE': 'Warehouse Queuing (Overload)',
                'PROBLEM': f'Queries waited {queued_overload/1000:.0f}s in queue due to overload.',
                'RECOMMENDATION': f'''Enable multi-cluster mode for better concurrency:
```sql
ALTER WAREHOUSE {warehouse} SET
    MIN_CLUSTER_COUNT = 1,
    MAX_CLUSTER_COUNT = 3,
    SCALING_POLICY = 'STANDARD';
```
This auto-scales when concurrency increases.'''
            })
        
        if queued_provision > 30000:
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'PROVISION_WAIT_SEC': round(queued_provision / 1000, 2),
                'QUERY_COUNT': query_count,
                'SEVERITY': 'MEDIUM',
                'ISSUE': 'Slow Warehouse Provisioning',
                'PROBLEM': f'Queries waited {queued_provision/1000:.0f}s for warehouse to start.',
                'RECOMMENDATION': f'''Increase auto-suspend time to keep warehouse warm:
```sql
ALTER WAREHOUSE {warehouse} SET AUTO_SUSPEND = 300;  -- 5 minutes
```
Balance between keeping warm (faster response) vs cost.'''
            })
    
    return pd.DataFrame(issues)

def analyze_repeated_expensive_queries(df):
    """Find repeated queries that are expensive - optimization targets"""
    issues = []
    
    if 'QUERY_PARAMETERIZED_HASH' not in df.columns:
        return pd.DataFrame(issues)
    
    grouped = df.groupby('QUERY_PARAMETERIZED_HASH').agg({
        'QUERY_ID': 'first',
        'QUERY_TEXT': 'first',
        'USER_NAME': 'first',
        'WAREHOUSE_NAME': 'first',
        'EXECUTION_TIME_SEC': ['sum', 'mean', 'count'],
        'BYTES_SCANNED': 'sum'
    }).reset_index()
    
    for idx, row in grouped.iterrows():
        exec_count = row[('EXECUTION_TIME_SEC', 'count')]
        total_time = row[('EXECUTION_TIME_SEC', 'sum')]
        avg_time = row[('EXECUTION_TIME_SEC', 'mean')]
        
        if exec_count >= 5 and total_time > 60:
            severity = 'HIGH' if total_time > 300 else 'MEDIUM'
            
            query_preview = str(row[('QUERY_TEXT', 'first')])[:200] + '...' if len(str(row[('QUERY_TEXT', 'first')])) > 200 else str(row[('QUERY_TEXT', 'first')])
            
            issues.append({
                'QUERY_ID': row[('QUERY_ID', 'first')],
                'USER_NAME': row[('USER_NAME', 'first')],
                'WAREHOUSE': row[('WAREHOUSE_NAME', 'first')],
                'EXECUTION_COUNT': exec_count,
                'TOTAL_TIME_SEC': round(total_time, 2),
                'AVG_TIME_SEC': round(avg_time, 2),
                'SEVERITY': severity,
                'ISSUE': 'Repeated Expensive Query',
                'QUERY_PREVIEW': query_preview,
                'PROBLEM': f'Same query ran {exec_count}x, consuming {total_time:.0f}s total compute time.',
                'RECOMMENDATION': '''Consider these optimizations:
```sql
-- 1. Create a Materialized View for pre-computed results:
CREATE MATERIALIZED VIEW mv_expensive_query AS
SELECT ... FROM ... WHERE ...;

-- 2. Use result caching (automatic for identical queries)
-- Ensure warehouse stays active for 24hr cache

-- 3. Create a scheduled task to pre-compute:
CREATE TASK update_summary_table
  WAREHOUSE = my_wh
  SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
  INSERT OVERWRITE INTO summary_table
  SELECT ... FROM ...;
```'''
            })
    
    return pd.DataFrame(issues)

def analyze_long_compilation(df):
    """Identify queries with excessive compilation time - query complexity issue"""
    issues = []
    
    for idx, row in df.iterrows():
        compilation_time = row['COMPILATION_TIME'] if pd.notna(row['COMPILATION_TIME']) else 0
        total_time = row['TOTAL_ELAPSED_TIME'] if pd.notna(row['TOTAL_ELAPSED_TIME']) else 1
        
        compilation_pct = (compilation_time / max(total_time, 1)) * 100
        
        if compilation_pct > 25 and compilation_time > 3000:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'COMPILATION_TIME_SEC': round(compilation_time / 1000, 2),
                'COMPILATION_PCT': round(compilation_pct, 1),
                'SEVERITY': 'MEDIUM',
                'ISSUE': 'Excessive Compilation Time',
                'PROBLEM': f'Compilation took {compilation_pct:.0f}% of total time ({compilation_time/1000:.1f}s). Query too complex.',
                'RECOMMENDATION': '''Simplify query structure:
```sql
-- Break complex CTEs into temp tables:
CREATE TEMP TABLE step1 AS SELECT ... FROM ...;
CREATE TEMP TABLE step2 AS SELECT ... FROM step1 ...;
SELECT ... FROM step2;

-- Avoid deeply nested subqueries
-- Reduce number of JOINs per query
-- Split into multiple simpler queries
```'''
            })
    
    return pd.DataFrame(issues)

def analyze_cache_efficiency(df):
    """Identify queries with low cache usage"""
    issues = []
    
    for idx, row in df.iterrows():
        cache_percentage = row['PERCENTAGE_SCANNED_FROM_CACHE'] if pd.notna(row['PERCENTAGE_SCANNED_FROM_CACHE']) else 0
        execution_time = row['EXECUTION_TIME_SEC']
        bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
        
        if cache_percentage < 10 and execution_time > 30 and bytes_scanned > 1073741824:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': execution_time,
                'CACHE_PERCENTAGE': round(cache_percentage, 1),
                'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                'SEVERITY': 'LOW',
                'ISSUE': 'Low Cache Utilization',
                'PROBLEM': f'Only {cache_percentage:.0f}% from cache. Cold data access is slower.',
                'RECOMMENDATION': '''Improve cache hit rate:
```sql
-- 1. Increase auto-suspend to keep warehouse warm:
ALTER WAREHOUSE my_wh SET AUTO_SUSPEND = 300;

-- 2. Schedule similar queries on same warehouse

-- 3. Use result caching (automatic):
-- Run identical queries within 24 hours

-- 4. Consolidate data access patterns:
-- Group queries that access same tables together
```'''
            })
    
    return pd.DataFrame(issues)

def analyze_query_retries(df):
    """Identify queries that had to retry - memory/resource issues"""
    issues = []
    
    if 'QUERY_RETRY_CAUSE' not in df.columns:
        return pd.DataFrame(issues)
    
    for idx, row in df.iterrows():
        retry_cause = row['QUERY_RETRY_CAUSE']
        retry_time = row['QUERY_RETRY_TIME'] if pd.notna(row['QUERY_RETRY_TIME']) else 0
        
        if pd.notna(retry_cause) and retry_cause:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'WAREHOUSE_SIZE': row['WAREHOUSE_SIZE'],
                'RETRY_CAUSE': str(retry_cause),
                'RETRY_TIME_SEC': round(retry_time / 1000, 2) if retry_time else 0,
                'SEVERITY': 'HIGH',
                'ISSUE': 'Query Retry Required',
                'PROBLEM': f'Query failed and retried due to: {retry_cause}',
                'RECOMMENDATION': '''Address the retry cause:
- If OOM (Out of Memory): Increase warehouse size
- If node failure: Check for complex operations
- Consider breaking query into smaller parts

```sql
-- Increase warehouse size:
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'LARGE';

-- Or break into smaller operations with temp tables
```'''
            })
    
    return pd.DataFrame(issues)

def analyze_full_table_scans(df):
    """Identify full table scans without effective filters - only for large, unfiltered scans"""
    issues = []
    
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        partitions_scanned = row['PARTITIONS_SCANNED'] if pd.notna(row['PARTITIONS_SCANNED']) else 0
        partitions_total = row['PARTITIONS_TOTAL'] if pd.notna(row['PARTITIONS_TOTAL']) else 0
        bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
        execution_time = row['EXECUTION_TIME_SEC']
        
        has_no_where = 'WHERE' not in query_text
        has_limit = 'LIMIT' in query_text
        is_select_query = query_text.strip().startswith('SELECT')
        
        full_scan_100_pct = (
            partitions_total > 200 and 
            partitions_scanned == partitions_total and
            has_no_where and
            not has_limit
        )
        
        very_large_unfiltered = (
            bytes_scanned > 53687091200 and  
            has_no_where and 
            not has_limit and
            is_select_query and
            execution_time > 120
        )
        
        if full_scan_100_pct or very_large_unfiltered:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': execution_time,
                'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                'PARTITIONS_SCANNED': partitions_scanned,
                'PARTITIONS_TOTAL': partitions_total,
                'SEVERITY': 'MEDIUM',
                'ISSUE': 'Full Table Scan (No Filter)',
                'PROBLEM': f'Scanned {bytes_scanned/(1024**3):.1f} GB ({partitions_scanned}/{partitions_total} partitions) without WHERE clause.',
                'RECOMMENDATION': '''If this isn't a required full-table operation, add filters:
```sql
-- Add date filters to reduce scan:
WHERE created_at >= DATEADD(day, -30, CURRENT_DATE())

-- Add partition key filters:
WHERE partition_date = '2024-01-01'

-- Or limit rows for exploration:
LIMIT 10000
```
Note: Some analytical workloads legitimately need full scans.'''
            })
    
    return pd.DataFrame(issues)

def analyze_cloud_services(df):
    """Identify queries with high cloud services usage"""
    issues = []
    
    for idx, row in df.iterrows():
        cloud_credits = row['CREDITS_USED_CLOUD_SERVICES'] if pd.notna(row['CREDITS_USED_CLOUD_SERVICES']) else 0
        
        if cloud_credits > 0.1:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'CLOUD_CREDITS': round(cloud_credits, 4),
                'SEVERITY': 'LOW',
                'ISSUE': 'High Cloud Services Usage',
                'PROBLEM': f'Query used {cloud_credits:.4f} cloud services credits (metadata operations).',
                'RECOMMENDATION': '''Reduce metadata operations:
- Batch small queries together
- Reduce SHOW/DESCRIBE commands
- Cache metadata in application layer
- Use larger transactions instead of many small ones'''
            })
    
    return pd.DataFrame(issues)

df = load_query_history()
warehouse_df = load_warehouse_metering()

if not df.empty:
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Queries (24h)", f"{len(df):,}")
    with col2:
        avg_time = df['EXECUTION_TIME_SEC'].mean()
        st.metric("Avg Execution Time", f"{avg_time:.2f}s")
    with col3:
        total_bytes = df['BYTES_SCANNED'].sum() / (1024**4)
        st.metric("Data Scanned", f"{total_bytes:.2f} TB")
    with col4:
        if not warehouse_df.empty:
            total_credits = warehouse_df['CREDITS_USED'].sum()
            st.metric("Credits Used", f"{total_credits:.2f}")
        else:
            st.metric("Credits Used", "N/A")
    with col5:
        slow_queries = len(df[df['EXECUTION_TIME_SEC'] > 60])
        st.metric("Slow Queries (>60s)", slow_queries)
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Overview", 
        "🔍 SQL Anti-Patterns", 
        "⚡ Performance Issues",
        "⚠️ Top Offenders", 
        "📈 Trends"
    ])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Credit Usage by Warehouse")
            if not warehouse_df.empty:
                warehouse_summary = warehouse_df.groupby('WAREHOUSE_NAME').agg({
                    'CREDITS_USED': 'sum'
                }).reset_index().sort_values('CREDITS_USED', ascending=True)
                
                fig = px.bar(
                    warehouse_summary.tail(10),
                    y='WAREHOUSE_NAME',
                    x='CREDITS_USED',
                    orientation='h',
                    title='Top 10 Warehouses by Credit Usage',
                    labels={'CREDITS_USED': 'Credits Used', 'WAREHOUSE_NAME': 'Warehouse'}
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Query Type Distribution")
            query_type_dist = df['QUERY_TYPE'].value_counts().head(8)
            fig = px.pie(
                values=query_type_dist.values,
                names=query_type_dist.index,
                title='Query Types'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Top Users by Execution Time")
            user_time = df.groupby('USER_NAME')['EXECUTION_TIME_SEC'].sum().sort_values(ascending=True).tail(10)
            fig = px.bar(
                x=user_time.values,
                y=user_time.index,
                orientation='h',
                labels={'x': 'Total Execution Time (s)', 'y': 'User'},
                title='Users by Total Compute Time'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Execution Time by Warehouse Size")
            size_order = ['X-SMALL', 'SMALL', 'MEDIUM', 'LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE']
            df_sized = df[df['WAREHOUSE_SIZE'].isin(size_order)]
            if not df_sized.empty:
                fig = px.box(
                    df_sized,
                    x='WAREHOUSE_SIZE',
                    y='EXECUTION_TIME_SEC',
                    title='Execution Time Distribution by Warehouse Size',
                    category_orders={'WAREHOUSE_SIZE': size_order}
                )
                fig.update_yaxis(type='log')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("🔍 SQL Anti-Pattern Analysis")
        st.markdown("*Detecting common SQL mistakes that waste credits*")
        
        select_star_issues = analyze_select_star(df)
        cartesian_issues = analyze_cartesian_joins(df)
        union_issues = analyze_union_vs_union_all(df)
        function_filter_issues = analyze_function_on_filter(df)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            count = len(select_star_issues)
            st.metric("SELECT * Issues", count, delta="Fix these!" if count > 0 else None, delta_color="inverse")
        with col2:
            count = len(cartesian_issues)
            st.metric("Join Issues", count, delta="Critical!" if count > 0 else None, delta_color="inverse")
        with col3:
            count = len(union_issues)
            st.metric("UNION Issues", count, delta="Check these" if count > 0 else None, delta_color="inverse")
        with col4:
            count = len(function_filter_issues)
            st.metric("Function Filter Issues", count, delta="Pruning disabled!" if count > 0 else None, delta_color="inverse")
        
        st.markdown("---")
        
        if not select_star_issues.empty:
            st.subheader("🔴 SELECT * Usage (Most Common Anti-Pattern)")
            st.markdown("*SELECT * forces Snowflake to read ALL columns, wasting I/O and credits.*")
            for idx, row in select_star_issues.head(10).iterrows():
                with st.expander(f"Query {row['QUERY_ID'][:20]}... by {row['USER_NAME']} ({row['BYTES_SCANNED_GB']} GB scanned)"):
                    st.markdown(f"**Problem:** {row['PROBLEM']}")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No SELECT * issues detected")
        
        st.markdown("---")
        
        if not cartesian_issues.empty:
            st.subheader("🔴 Cartesian Join / Join Issues")
            st.markdown("*Missing or incorrect join conditions cause row explosion and massive credit usage.*")
            for idx, row in cartesian_issues.head(10).iterrows():
                with st.expander(f"Query {row['QUERY_ID'][:20]}... - {row['PROBLEM']}"):
                    st.markdown(f"**Severity:** {row['SEVERITY']}")
                    st.markdown(f"**Rows Produced:** {row['ROWS_PRODUCED']:,}")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No join issues detected")
        
        st.markdown("---")
        
        if not function_filter_issues.empty:
            st.subheader("🟠 Functions on Filter Columns")
            st.markdown("*Functions like YEAR(), DATE() on WHERE columns DISABLE partition pruning.*")
            for idx, row in function_filter_issues.head(10).iterrows():
                with st.expander(f"Query {row['QUERY_ID'][:20]}... - {row['FUNCTIONS_DETECTED']}"):
                    st.markdown(f"**Problem:** {row['PROBLEM']}")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No function-on-filter issues detected")
        
        st.markdown("---")
        
        if not union_issues.empty:
            st.subheader("🟡 UNION vs UNION ALL")
            st.markdown("*UNION performs costly duplicate elimination. Use UNION ALL if duplicates are OK.*")
            st.dataframe(union_issues[['QUERY_ID', 'USER_NAME', 'EXECUTION_TIME_SEC', 'ISSUE']], use_container_width=True)
        else:
            st.success("No UNION issues detected")
    
    with tab3:
        st.subheader("⚡ Performance Issue Analysis")
        st.markdown("*Infrastructure and resource-related problems*")
        
        spilling_issues = analyze_spilling(df)
        pruning_issues = analyze_poor_pruning(df)
        warehouse_issues = analyze_warehouse_sizing(df)
        compilation_issues = analyze_long_compilation(df)
        cache_issues = analyze_cache_efficiency(df)
        repeated_issues = analyze_repeated_expensive_queries(df)
        retry_issues = analyze_query_retries(df)
        full_scan_issues = analyze_full_table_scans(df)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            count = len(spilling_issues)
            st.metric("Memory Spilling", count, delta="Upgrade warehouse!" if count > 0 else None, delta_color="inverse")
        with col2:
            count = len(pruning_issues)
            st.metric("Poor Pruning", count, delta="Add filters!" if count > 0 else None, delta_color="inverse")
        with col3:
            count = len(warehouse_issues)
            st.metric("Warehouse Issues", count, delta="Resize!" if count > 0 else None, delta_color="inverse")
        with col4:
            count = len(repeated_issues)
            st.metric("Repeated Expensive", count, delta="Cache these!" if count > 0 else None, delta_color="inverse")
        
        st.markdown("---")
        
        if not spilling_issues.empty:
            st.subheader("🔴 Memory Spilling (Critical)")
            st.markdown("*Queries exceeding memory spill to disk, causing severe slowdowns.*")
            for idx, row in spilling_issues.head(10).iterrows():
                with st.expander(f"Query {row['QUERY_ID'][:20]}... - {row['LOCAL_SPILL_GB'] + row['REMOTE_SPILL_GB']:.2f} GB spilled"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Local Spill", f"{row['LOCAL_SPILL_GB']} GB")
                    with col2:
                        st.metric("Remote Spill", f"{row['REMOTE_SPILL_GB']} GB", delta="Critical!" if row['REMOTE_SPILL_GB'] > 0 else None, delta_color="inverse")
                    st.markdown(f"**Current Warehouse:** {row['WAREHOUSE_SIZE']}")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No spilling issues detected")
        
        st.markdown("---")
        
        if not pruning_issues.empty:
            st.subheader("🟠 Poor Partition Pruning")
            st.markdown("*Queries scanning too many partitions waste I/O and credits.*")
            for idx, row in pruning_issues.head(10).iterrows():
                with st.expander(f"Query {row['QUERY_ID'][:20]}... - {row['SCAN_PERCENTAGE']:.0f}% of partitions scanned"):
                    st.markdown(f"**Partitions:** {row['PARTITIONS_SCANNED']:,} / {row['PARTITIONS_TOTAL']:,}")
                    st.markdown(f"**Data Scanned:** {row['BYTES_SCANNED_GB']} GB")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No significant pruning issues detected")
        
        st.markdown("---")
        
        if not warehouse_issues.empty:
            st.subheader("🟡 Warehouse Sizing Issues")
            st.markdown("*Oversized warehouses waste credits, undersized ones cause queuing.*")
            for idx, row in warehouse_issues.iterrows():
                with st.expander(f"{row['WAREHOUSE']} - {row['ISSUE']}"):
                    st.markdown(f"**Problem:** {row['PROBLEM']}")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No warehouse sizing issues detected")
        
        st.markdown("---")
        
        if not repeated_issues.empty:
            st.subheader("🔵 Repeated Expensive Queries")
            st.markdown("*Same queries running repeatedly - consider caching or materialized views.*")
            for idx, row in repeated_issues.head(10).iterrows():
                with st.expander(f"Query ran {row['EXECUTION_COUNT']}x, total {row['TOTAL_TIME_SEC']:.0f}s compute"):
                    st.markdown(f"**Preview:** `{row['QUERY_PREVIEW']}`")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No repeated expensive queries detected")
        
        st.markdown("---")
        
        if not full_scan_issues.empty:
            st.subheader("⚪ Full Table Scans")
            for idx, row in full_scan_issues.head(10).iterrows():
                with st.expander(f"Query {row['QUERY_ID'][:20]}... - {row['BYTES_SCANNED_GB']} GB scanned"):
                    st.markdown(f"**Problem:** {row['PROBLEM']}")
                    st.markdown(f"**Recommendation:**")
                    st.code(row['RECOMMENDATION'], language='sql')
        else:
            st.success("No full table scan issues detected")
    
    with tab4:
        st.subheader("⚠️ Top 25 Most Expensive Queries")
        
        top_queries = df.nlargest(25, 'EXECUTION_TIME_SEC')[
            ['QUERY_ID', 'USER_NAME', 'WAREHOUSE_NAME', 'WAREHOUSE_SIZE', 
             'EXECUTION_TIME_SEC', 'BYTES_SCANNED', 'ROWS_PRODUCED', 'QUERY_TYPE']
        ].copy()
        
        top_queries['BYTES_SCANNED_GB'] = (top_queries['BYTES_SCANNED'] / (1024**3)).round(2)
        top_queries['EXECUTION_TIME_SEC'] = top_queries['EXECUTION_TIME_SEC'].round(2)
        top_queries = top_queries.drop('BYTES_SCANNED', axis=1)
        
        st.dataframe(top_queries, use_container_width=True, height=400)
        
        st.markdown("---")
        
        st.subheader("Execution Time Distribution")
        fig = px.histogram(
            df,
            x='EXECUTION_TIME_SEC',
            nbins=50,
            title='Query Execution Time Distribution',
            labels={'EXECUTION_TIME_SEC': 'Execution Time (seconds)'},
            log_y=True
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        st.subheader("Data Scanned vs Execution Time")
        sample_df = df.sample(min(1000, len(df))) if len(df) > 1000 else df
        fig = px.scatter(
            sample_df,
            x='BYTES_SCANNED',
            y='EXECUTION_TIME_SEC',
            color='WAREHOUSE_SIZE',
            hover_data=['USER_NAME', 'QUERY_TYPE'],
            title='Data Scanned vs Execution Time',
            labels={'BYTES_SCANNED': 'Bytes Scanned', 'EXECUTION_TIME_SEC': 'Execution Time (s)'}
        )
        fig.update_xaxes(type='log')
        fig.update_yaxes(type='log')
        st.plotly_chart(fig, use_container_width=True)
    
    with tab5:
        st.subheader("📈 Query Volume Over Time")
        
        df['HOUR'] = pd.to_datetime(df['START_TIME']).dt.floor('H')
        hourly_queries = df.groupby('HOUR').agg({
            'QUERY_ID': 'count',
            'EXECUTION_TIME_SEC': 'sum'
        }).reset_index()
        hourly_queries.columns = ['HOUR', 'QUERY_COUNT', 'TOTAL_EXECUTION_TIME']
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=hourly_queries['HOUR'], y=hourly_queries['QUERY_COUNT'], name='Query Count'))
        fig.add_trace(go.Scatter(x=hourly_queries['HOUR'], y=hourly_queries['TOTAL_EXECUTION_TIME'], 
                                  name='Total Execution Time', yaxis='y2', line=dict(color='red')))
        fig.update_layout(
            title='Queries per Hour & Total Execution Time',
            yaxis=dict(title='Query Count'),
            yaxis2=dict(title='Total Execution Time (s)', overlaying='y', side='right')
        )
        st.plotly_chart(fig, use_container_width=True)
        
        if not warehouse_df.empty:
            st.subheader("💰 Credit Usage Trend")
            warehouse_df['HOUR'] = pd.to_datetime(warehouse_df['START_TIME']).dt.floor('H')
            hourly_credits = warehouse_df.groupby('HOUR')['CREDITS_USED'].sum().reset_index()
            
            fig = px.area(
                hourly_credits,
                x='HOUR',
                y='CREDITS_USED',
                title='Credit Usage per Hour',
                labels={'HOUR': 'Time', 'CREDITS_USED': 'Credits Used'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("👤 Activity by User Over Time")
        user_hourly = df.groupby(['HOUR', 'USER_NAME']).size().reset_index(name='QUERY_COUNT')
        top_users = df['USER_NAME'].value_counts().head(5).index.tolist()
        user_hourly_top = user_hourly[user_hourly['USER_NAME'].isin(top_users)]
        
        if not user_hourly_top.empty:
            fig = px.line(
                user_hourly_top,
                x='HOUR',
                y='QUERY_COUNT',
                color='USER_NAME',
                title='Query Activity by Top 5 Users',
                labels={'HOUR': 'Time', 'QUERY_COUNT': 'Query Count', 'USER_NAME': 'User'}
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("💡 Executive Summary & Recommendations")
    
    all_issues_count = (
        len(select_star_issues) + len(cartesian_issues) + len(union_issues) + 
        len(function_filter_issues) + len(spilling_issues) + len(pruning_issues) + 
        len(warehouse_issues) + len(repeated_issues) + len(full_scan_issues)
    )
    
    if all_issues_count > 0:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("### Priority Fixes (Highest Impact)")
            
            priority_fixes = []
            
            if len(cartesian_issues) > 0:
                priority_fixes.append(("🔴 CRITICAL", f"{len(cartesian_issues)} Cartesian Join Issues", 
                    "Review JOIN conditions immediately - these cause massive credit waste"))
            
            if len(spilling_issues) > 0:
                critical_spills = len(spilling_issues[spilling_issues['SEVERITY'] == 'CRITICAL'])
                priority_fixes.append(("🔴 CRITICAL" if critical_spills > 0 else "🟠 HIGH", 
                    f"{len(spilling_issues)} Memory Spilling Issues",
                    "Upgrade warehouse sizes or optimize queries"))
            
            if len(select_star_issues) > 0:
                priority_fixes.append(("🟠 HIGH", f"{len(select_star_issues)} SELECT * Usages",
                    "Replace with specific column lists"))
            
            if len(function_filter_issues) > 0:
                priority_fixes.append(("🟠 HIGH", f"{len(function_filter_issues)} Functions on Filters",
                    "Rewrite WHERE clauses to enable pruning"))
            
            if len(pruning_issues) > 0:
                priority_fixes.append(("🟡 MEDIUM", f"{len(pruning_issues)} Poor Pruning Issues",
                    "Add clustering keys or better filters"))
            
            if len(warehouse_issues) > 0:
                priority_fixes.append(("🟡 MEDIUM", f"{len(warehouse_issues)} Warehouse Sizing Issues",
                    "Right-size warehouses based on workload"))
            
            if len(repeated_issues) > 0:
                priority_fixes.append(("🔵 LOW", f"{len(repeated_issues)} Repeated Expensive Queries",
                    "Consider materialized views or caching"))
            
            for severity, issue, action in priority_fixes[:7]:
                st.markdown(f"{severity} **{issue}** → {action}")
        
        with col2:
            st.markdown("### Quick Stats")
            st.metric("Total Issues Found", all_issues_count)
            critical = len(cartesian_issues) + len(spilling_issues[spilling_issues['SEVERITY'] == 'CRITICAL']) if not spilling_issues.empty else 0
            st.metric("Critical Issues", critical, delta="Fix now!" if critical > 0 else None, delta_color="inverse")
    else:
        st.success("🎉 No major issues detected! Your queries are running efficiently.")
    
    st.markdown("---")
    st.info("""
    **Best Practices for Credit Optimization:**
    - ✅ Always specify columns instead of SELECT *
    - ✅ Use explicit JOIN conditions with ON clause
    - ✅ Avoid functions on WHERE clause columns
    - ✅ Set auto-suspend to 1-5 minutes for warehouses  
    - ✅ Start with X-Small/Small and scale based on spilling
    - ✅ Use clustering keys on frequently filtered columns
    - ✅ Leverage result caching for repeated queries
    - ✅ Break complex queries into smaller steps
    - ✅ Use UNION ALL instead of UNION when possible
    - ✅ Set statement timeouts to prevent runaway queries
    """)

else:
    st.warning("No query history data available for the past 24 hours.")
    st.info("""
    **Possible causes:**
    - No queries ran in the last 24 hours
    - Missing ACCOUNTADMIN role or IMPORTED PRIVILEGES on SNOWFLAKE database
    - ACCOUNT_USAGE has up to 45-minute latency
    
    **To grant access:**
    ```sql
    GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE your_role;
    ```
    """)
