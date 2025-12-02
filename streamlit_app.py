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
st.markdown("**Identify and fix credit-wasting queries**")
st.markdown("---")

if 'active_section' not in st.session_state:
    st.session_state.active_section = None

@st.cache_data(ttl=300)
def load_query_history():
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
    issues = []
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        select_star_patterns = [r'SELECT\s+\*\s+FROM', r'SELECT\s+[A-Z_]+\.\*']
        has_select_star = any(re.search(pattern, query_text) for pattern in select_star_patterns)
        if has_select_star:
            bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
            severity = 'HIGH' if bytes_scanned > 1073741824 else 'MEDIUM'
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                'SEVERITY': severity,
                'ISSUE': 'SELECT * Usage',
                'RECOMMENDATION': 'Replace SELECT * with specific columns to reduce I/O'
            })
    return pd.DataFrame(issues)

def analyze_cartesian_joins(df):
    issues = []
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        has_join = 'JOIN' in query_text
        has_on_or_using = ' ON ' in query_text or 'USING' in query_text
        has_comma_join = re.search(r'FROM\s+\w+\s*,\s*\w+', query_text) and 'WHERE' not in query_text
        has_cross_join = 'CROSS JOIN' in query_text
        has_or_in_join = re.search(r'JOIN[^;]*?ON[^;]*?\sOR\s', query_text)
        rows_produced = row['ROWS_PRODUCED'] if pd.notna(row['ROWS_PRODUCED']) else 0
        bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 0
        execution_time = row['EXECUTION_TIME_SEC']
        high_row_explosion = (rows_produced > 10000000 and bytes_scanned > 0 and 
                              execution_time > 60 and (rows_produced / max(bytes_scanned, 1)) > 100)
        missing_join_condition = (has_join and not has_on_or_using) or has_comma_join
        if missing_join_condition or has_cross_join or high_row_explosion or has_or_in_join:
            severity = 'CRITICAL' if (missing_join_condition or has_cross_join) else 'HIGH'
            if missing_join_condition:
                problem = "Missing ON/USING clause"
            elif has_cross_join:
                problem = "CROSS JOIN detected"
            elif has_or_in_join:
                problem = "OR in JOIN clause"
            else:
                problem = f"Row explosion ({rows_produced:,} rows)"
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': execution_time,
                'ROWS_PRODUCED': rows_produced,
                'SEVERITY': severity,
                'PROBLEM': problem,
                'RECOMMENDATION': 'Add explicit JOIN conditions with ON clause'
            })
    return pd.DataFrame(issues)

def analyze_union_vs_union_all(df):
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
                'RECOMMENDATION': 'Use UNION ALL if duplicates are acceptable (2-3x faster)'
            })
    return pd.DataFrame(issues)

def analyze_function_on_filter(df):
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
                'FUNCTIONS': ', '.join(detected_functions),
                'PARTITIONS_SCANNED': partitions_scanned,
                'SEVERITY': severity,
                'RECOMMENDATION': 'Rewrite WHERE to use date ranges instead of functions'
            })
    return pd.DataFrame(issues)

def analyze_spilling(df):
    issues = []
    for idx, row in df.iterrows():
        local_spill = row['BYTES_SPILLED_TO_LOCAL_STORAGE'] if pd.notna(row['BYTES_SPILLED_TO_LOCAL_STORAGE']) else 0
        remote_spill = row['BYTES_SPILLED_TO_REMOTE_STORAGE'] if pd.notna(row['BYTES_SPILLED_TO_REMOTE_STORAGE']) else 0
        if local_spill > 0 or remote_spill > 0:
            severity = 'CRITICAL' if remote_spill > 0 else 'HIGH'
            total_spill_gb = (local_spill + remote_spill) / (1024**3)
            current_size = row['WAREHOUSE_SIZE'] if pd.notna(row['WAREHOUSE_SIZE']) else 'UNKNOWN'
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'WAREHOUSE_SIZE': current_size,
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'LOCAL_SPILL_GB': round(local_spill / (1024**3), 2),
                'REMOTE_SPILL_GB': round(remote_spill / (1024**3), 2),
                'SEVERITY': severity,
                'RECOMMENDATION': f'Upgrade warehouse from {current_size} or optimize query'
            })
    return pd.DataFrame(issues)

def analyze_poor_pruning(df):
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
                    'PARTITIONS': f"{partitions_scanned:,}/{partitions_total:,}",
                    'SCAN_PCT': f"{scan_percentage:.0f}%",
                    'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                    'SEVERITY': severity,
                    'RECOMMENDATION': 'Add clustering keys or filter on clustered columns'
                })
    return pd.DataFrame(issues)

def analyze_warehouse_sizing(df):
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
        if avg_exec < 3 and size in ['LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE']:
            credits_per_hour = {'LARGE': 8, 'X-LARGE': 16, '2X-LARGE': 32, '3X-LARGE': 64, '4X-LARGE': 128}
            current_credits = credits_per_hour.get(size, 8)
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'AVG_EXEC_SEC': round(avg_exec, 2),
                'QUERY_COUNT': query_count,
                'ISSUE_TYPE': 'Oversized',
                'SEVERITY': 'MEDIUM',
                'RECOMMENDATION': f'Downsize from {size} to SMALL/MEDIUM (saves {current_credits-2} credits/hr)'
            })
        if queued_overload > 60000:
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'QUEUED_SEC': round(queued_overload / 1000, 2),
                'QUERY_COUNT': query_count,
                'ISSUE_TYPE': 'Queuing',
                'SEVERITY': 'HIGH',
                'RECOMMENDATION': 'Enable multi-cluster scaling or increase warehouse size'
            })
    return pd.DataFrame(issues)

def analyze_repeated_expensive_queries(df):
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
            query_preview = str(row[('QUERY_TEXT', 'first')])[:100] + '...'
            issues.append({
                'QUERY_ID': row[('QUERY_ID', 'first')],
                'USER_NAME': row[('USER_NAME', 'first')],
                'WAREHOUSE': row[('WAREHOUSE_NAME', 'first')],
                'EXEC_COUNT': exec_count,
                'TOTAL_TIME_SEC': round(total_time, 2),
                'AVG_TIME_SEC': round(avg_time, 2),
                'SEVERITY': severity,
                'QUERY_PREVIEW': query_preview,
                'RECOMMENDATION': 'Create materialized view or cache results'
            })
    return pd.DataFrame(issues)

def analyze_long_compilation(df):
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
                'COMPILATION_SEC': round(compilation_time / 1000, 2),
                'COMPILATION_PCT': f"{compilation_pct:.0f}%",
                'SEVERITY': 'MEDIUM',
                'RECOMMENDATION': 'Simplify query structure or break into temp tables'
            })
    return pd.DataFrame(issues)

def analyze_cache_efficiency(df):
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
                'CACHE_PCT': f"{cache_percentage:.0f}%",
                'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                'SEVERITY': 'LOW',
                'RECOMMENDATION': 'Increase auto-suspend time to keep warehouse warm'
            })
    return pd.DataFrame(issues)

def analyze_full_table_scans(df):
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
        full_scan_100_pct = (partitions_total > 200 and partitions_scanned == partitions_total and
                            has_no_where and not has_limit)
        very_large_unfiltered = (bytes_scanned > 53687091200 and has_no_where and 
                                 not has_limit and is_select_query and execution_time > 120)
        if full_scan_100_pct or very_large_unfiltered:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': execution_time,
                'BYTES_SCANNED_GB': round(bytes_scanned / (1024**3), 2),
                'PARTITIONS': f"{partitions_scanned}/{partitions_total}",
                'SEVERITY': 'MEDIUM',
                'RECOMMENDATION': 'Add WHERE clause or LIMIT for exploratory queries'
            })
    return pd.DataFrame(issues)

@st.cache_data(ttl=300)
def run_all_analyses(_df):
    """Run all analyses once and return counts and DataFrames"""
    results = {
        'select_star': analyze_select_star(_df),
        'cartesian': analyze_cartesian_joins(_df),
        'union': analyze_union_vs_union_all(_df),
        'function_filter': analyze_function_on_filter(_df),
        'spilling': analyze_spilling(_df),
        'pruning': analyze_poor_pruning(_df),
        'warehouse': analyze_warehouse_sizing(_df),
        'repeated': analyze_repeated_expensive_queries(_df),
        'compilation': analyze_long_compilation(_df),
        'cache': analyze_cache_efficiency(_df),
        'full_scan': analyze_full_table_scans(_df),
    }
    
    counts = {name: len(df_result) for name, df_result in results.items()}
    
    counts['sql_antipatterns'] = counts['select_star'] + counts['cartesian'] + counts['union'] + counts['function_filter']
    counts['performance'] = counts['spilling'] + counts['pruning'] + counts['warehouse'] + counts['compilation'] + counts['cache']
    counts['operational'] = counts['repeated'] + counts['full_scan']
    counts['total'] = sum(counts[k] for k in ['select_star', 'cartesian', 'union', 'function_filter', 
                                               'spilling', 'pruning', 'warehouse', 'compilation', 
                                               'cache', 'repeated', 'full_scan'])
    
    critical_count = 0
    for name, df_result in results.items():
        if not df_result.empty and 'SEVERITY' in df_result.columns:
            critical_count += len(df_result[df_result['SEVERITY'] == 'CRITICAL'])
    counts['critical'] = critical_count
    
    return results, counts

df = load_query_history()
warehouse_df = load_warehouse_metering()

if not df.empty:
    results, counts = run_all_analyses(df)
    
    st.subheader("📊 Issue Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Queries", f"{len(df):,}")
    with col2:
        st.metric("Total Issues", counts['total'], delta="needs attention" if counts['total'] > 0 else None, delta_color="inverse")
    with col3:
        st.metric("Critical Issues", counts['critical'], delta="fix now!" if counts['critical'] > 0 else None, delta_color="inverse")
    with col4:
        if not warehouse_df.empty:
            total_credits = warehouse_df['CREDITS_USED'].sum()
            st.metric("Credits Used (24h)", f"{total_credits:.2f}")
        else:
            st.metric("Credits Used", "N/A")
    with col5:
        avg_time = df['EXECUTION_TIME_SEC'].mean()
        st.metric("Avg Query Time", f"{avg_time:.1f}s")
    
    st.markdown("---")
    
    st.subheader("🔍 Issue Categories")
    st.markdown("*Click a category to view detailed issues*")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button(f"🔴 SQL Anti-Patterns\n({counts['sql_antipatterns']} issues)", use_container_width=True):
            st.session_state.active_section = 'sql_antipatterns'
    
    with col2:
        if st.button(f"⚡ Performance Issues\n({counts['performance']} issues)", use_container_width=True):
            st.session_state.active_section = 'performance'
    
    with col3:
        if st.button(f"🔄 Operational Issues\n({counts['operational']} issues)", use_container_width=True):
            st.session_state.active_section = 'operational'
    
    with col4:
        if st.button(f"📈 Trends & Charts", use_container_width=True):
            st.session_state.active_section = 'trends'
    
    st.markdown("---")
    
    if st.session_state.active_section == 'sql_antipatterns':
        st.subheader("🔴 SQL Anti-Pattern Issues")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("SELECT *", counts['select_star'])
        with col2:
            st.metric("Join Issues", counts['cartesian'])
        with col3:
            st.metric("UNION Issues", counts['union'])
        with col4:
            st.metric("Function on Filter", counts['function_filter'])
        
        if counts['select_star'] > 0:
            with st.expander(f"📌 SELECT * Usage ({counts['select_star']} queries)", expanded=True):
                st.markdown("**Problem:** SELECT * scans all columns, wasting I/O")
                st.markdown("**Fix:** Replace with specific column names")
                st.code("""-- Instead of:
SELECT * FROM my_table

-- Use:
SELECT column1, column2 FROM my_table""", language='sql')
                st.dataframe(results['select_star'][['QUERY_ID', 'USER_NAME', 'WAREHOUSE', 'BYTES_SCANNED_GB', 'SEVERITY']].head(10), use_container_width=True)
        
        if counts['cartesian'] > 0:
            with st.expander(f"⚠️ Cartesian Join Issues ({counts['cartesian']} queries)", expanded=True):
                st.markdown("**Problem:** Missing or incorrect JOIN conditions cause row explosion")
                st.markdown("**Fix:** Add explicit ON clauses to all JOINs")
                st.code("""-- Instead of:
SELECT * FROM orders, customers

-- Use:
SELECT o.*, c.name 
FROM orders o
JOIN customers c ON o.customer_id = c.id""", language='sql')
                st.dataframe(results['cartesian'][['QUERY_ID', 'USER_NAME', 'PROBLEM', 'ROWS_PRODUCED', 'SEVERITY']].head(10), use_container_width=True)
        
        if counts['function_filter'] > 0:
            with st.expander(f"🔶 Functions on Filter Columns ({counts['function_filter']} queries)", expanded=True):
                st.markdown("**Problem:** Functions like YEAR(), DATE() on WHERE columns disable partition pruning")
                st.markdown("**Fix:** Use date range filters instead")
                st.code("""-- Instead of:
WHERE YEAR(order_date) = 2024

-- Use:
WHERE order_date >= '2024-01-01' 
  AND order_date < '2025-01-01'""", language='sql')
                st.dataframe(results['function_filter'][['QUERY_ID', 'USER_NAME', 'FUNCTIONS', 'PARTITIONS_SCANNED', 'SEVERITY']].head(10), use_container_width=True)
        
        if counts['union'] > 0:
            with st.expander(f"🔵 UNION vs UNION ALL ({counts['union']} queries)"):
                st.markdown("**Problem:** UNION removes duplicates (slow). UNION ALL keeps all rows (fast)")
                st.dataframe(results['union'][['QUERY_ID', 'USER_NAME', 'EXECUTION_TIME_SEC']].head(10), use_container_width=True)
        
        if counts['sql_antipatterns'] == 0:
            st.success("No SQL anti-pattern issues detected!")
    
    elif st.session_state.active_section == 'performance':
        st.subheader("⚡ Performance Issues")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Memory Spilling", counts['spilling'])
        with col2:
            st.metric("Poor Pruning", counts['pruning'])
        with col3:
            st.metric("Warehouse Issues", counts['warehouse'])
        with col4:
            st.metric("Slow Compilation", counts['compilation'])
        with col5:
            st.metric("Low Cache", counts['cache'])
        
        if counts['spilling'] > 0:
            with st.expander(f"🔴 Memory Spilling ({counts['spilling']} queries)", expanded=True):
                st.markdown("**Problem:** Query exceeds memory, spilling to disk slows execution")
                st.markdown("**Fix:** Upgrade warehouse size OR break query into smaller steps")
                st.code("""-- Option 1: Upgrade warehouse
ALTER WAREHOUSE my_wh SET WAREHOUSE_SIZE = 'LARGE';

-- Option 2: Break into temp tables
CREATE TEMP TABLE step1 AS SELECT ... WHERE date_filter;
SELECT * FROM step1 JOIN ...;""", language='sql')
                display_cols = ['QUERY_ID', 'USER_NAME', 'WAREHOUSE_SIZE', 'LOCAL_SPILL_GB', 'REMOTE_SPILL_GB', 'SEVERITY']
                st.dataframe(results['spilling'][display_cols].head(10), use_container_width=True)
        
        if counts['pruning'] > 0:
            with st.expander(f"🟠 Poor Partition Pruning ({counts['pruning']} queries)", expanded=True):
                st.markdown("**Problem:** Scanning too many partitions wastes I/O and credits")
                st.markdown("**Fix:** Add filters on clustered columns or add clustering keys")
                st.code("""-- Check clustering:
SELECT SYSTEM$CLUSTERING_INFORMATION('my_table');

-- Add clustering key:
ALTER TABLE my_table CLUSTER BY (date_column);

-- Always filter on clustered columns:
SELECT * FROM my_table WHERE date_column >= '2024-01-01'""", language='sql')
                st.dataframe(results['pruning'][['QUERY_ID', 'USER_NAME', 'PARTITIONS', 'SCAN_PCT', 'BYTES_SCANNED_GB']].head(10), use_container_width=True)
        
        if counts['warehouse'] > 0:
            with st.expander(f"🟡 Warehouse Sizing Issues ({counts['warehouse']} issues)", expanded=True):
                st.markdown("**Problem:** Oversized warehouses waste credits; undersized cause queuing")
                st.dataframe(results['warehouse'].head(10), use_container_width=True)
        
        if counts['compilation'] > 0:
            with st.expander(f"🔵 Slow Query Compilation ({counts['compilation']} queries)"):
                st.markdown("**Problem:** Complex queries take too long to compile")
                st.markdown("**Fix:** Simplify query or use temp tables")
                st.dataframe(results['compilation'][['QUERY_ID', 'USER_NAME', 'COMPILATION_SEC', 'COMPILATION_PCT']].head(10), use_container_width=True)
        
        if counts['cache'] > 0:
            with st.expander(f"⚪ Low Cache Utilization ({counts['cache']} queries)"):
                st.markdown("**Problem:** Cold data access is slower")
                st.markdown("**Fix:** Increase warehouse auto-suspend time")
                st.dataframe(results['cache'][['QUERY_ID', 'USER_NAME', 'CACHE_PCT', 'BYTES_SCANNED_GB']].head(10), use_container_width=True)
        
        if counts['performance'] == 0:
            st.success("No performance issues detected!")
    
    elif st.session_state.active_section == 'operational':
        st.subheader("🔄 Operational Issues")
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Repeated Expensive Queries", counts['repeated'])
        with col2:
            st.metric("Full Table Scans", counts['full_scan'])
        
        if counts['repeated'] > 0:
            with st.expander(f"🔄 Repeated Expensive Queries ({counts['repeated']} patterns)", expanded=True):
                st.markdown("**Problem:** Same costly query runs multiple times")
                st.markdown("**Fix:** Create materialized view or cache results")
                st.code("""-- Create materialized view:
CREATE MATERIALIZED VIEW mv_summary AS
SELECT ... FROM ... WHERE ...;

-- Or use result caching (automatic for identical queries)""", language='sql')
                display_cols = ['QUERY_ID', 'USER_NAME', 'EXEC_COUNT', 'TOTAL_TIME_SEC', 'AVG_TIME_SEC']
                st.dataframe(results['repeated'][display_cols].head(10), use_container_width=True)
        
        if counts['full_scan'] > 0:
            with st.expander(f"📊 Full Table Scans ({counts['full_scan']} queries)", expanded=True):
                st.markdown("**Problem:** Large scans without filters waste resources")
                st.markdown("**Fix:** Add WHERE clause or LIMIT")
                st.dataframe(results['full_scan'][['QUERY_ID', 'USER_NAME', 'BYTES_SCANNED_GB', 'PARTITIONS']].head(10), use_container_width=True)
        
        if counts['operational'] == 0:
            st.success("No operational issues detected!")
    
    elif st.session_state.active_section == 'trends':
        st.subheader("📈 Trends & Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Query Volume Over Time**")
            df['HOUR'] = pd.to_datetime(df['START_TIME']).dt.floor('H')
            hourly = df.groupby('HOUR').size().reset_index(name='COUNT')
            fig = px.bar(hourly, x='HOUR', y='COUNT', title='Queries per Hour')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("**Credit Usage by Warehouse**")
            if not warehouse_df.empty:
                wh_credits = warehouse_df.groupby('WAREHOUSE_NAME')['CREDITS_USED'].sum().sort_values(ascending=True).tail(10)
                fig = px.bar(x=wh_credits.values, y=wh_credits.index, orientation='h', 
                            title='Top 10 Warehouses by Credits', labels={'x': 'Credits', 'y': 'Warehouse'})
                st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Top 10 Most Expensive Queries**")
            top_queries = df.nlargest(10, 'EXECUTION_TIME_SEC')[['QUERY_ID', 'USER_NAME', 'WAREHOUSE_NAME', 'EXECUTION_TIME_SEC']]
            top_queries['EXECUTION_TIME_SEC'] = top_queries['EXECUTION_TIME_SEC'].round(1)
            st.dataframe(top_queries, use_container_width=True)
        
        with col2:
            st.markdown("**Top Users by Compute Time**")
            user_time = df.groupby('USER_NAME')['EXECUTION_TIME_SEC'].sum().sort_values(ascending=False).head(10)
            fig = px.pie(values=user_time.values, names=user_time.index, title='Compute Time by User')
            st.plotly_chart(fig, use_container_width=True)
        
        if not warehouse_df.empty:
            st.markdown("**Credit Usage Trend (24h)**")
            warehouse_df['HOUR'] = pd.to_datetime(warehouse_df['START_TIME']).dt.floor('H')
            hourly_credits = warehouse_df.groupby('HOUR')['CREDITS_USED'].sum().reset_index()
            fig = px.area(hourly_credits, x='HOUR', y='CREDITS_USED', title='Credits Used per Hour')
            st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.info("👆 Click a category above to view detailed issues")
        
        st.subheader("📋 Quick Summary")
        
        if counts['total'] > 0:
            st.markdown("**Priority Fixes:**")
            
            priority_items = []
            if counts['cartesian'] > 0:
                priority_items.append(f"🔴 **{counts['cartesian']} Cartesian Join Issues** - Missing JOIN conditions cause massive waste")
            if counts['spilling'] > 0:
                priority_items.append(f"🔴 **{counts['spilling']} Memory Spilling Issues** - Upgrade warehouse or optimize queries")
            if counts['select_star'] > 0:
                priority_items.append(f"🟠 **{counts['select_star']} SELECT * Queries** - Use specific column names")
            if counts['function_filter'] > 0:
                priority_items.append(f"🟠 **{counts['function_filter']} Function Filter Issues** - Rewrite WHERE clauses")
            if counts['pruning'] > 0:
                priority_items.append(f"🟡 **{counts['pruning']} Pruning Issues** - Add clustering keys")
            if counts['repeated'] > 0:
                priority_items.append(f"🔵 **{counts['repeated']} Repeated Queries** - Consider materialized views")
            
            for item in priority_items[:6]:
                st.markdown(item)
        else:
            st.success("🎉 No issues detected! Your queries are running efficiently.")
    
    st.markdown("---")
    st.caption("Data refreshes every 5 minutes. ACCOUNT_USAGE has up to 45-minute latency.")

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
