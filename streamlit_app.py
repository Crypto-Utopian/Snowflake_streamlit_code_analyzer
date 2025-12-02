import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_icon="❄️", page_title="Snowflake Credit Usage Analyzer")

session = get_active_session()

st.title("❄️ Snowflake Credit Usage Analyzer")
st.markdown("**Analyze query performance and identify credit optimization opportunities**")
st.markdown("---")

@st.cache_data(ttl=300)
def load_query_history():
    """Load query history from the past 24 hours"""
    query = """
    SELECT 
        QUERY_ID,
        QUERY_TEXT,
        QUERY_TYPE,
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
        BYTES_SCANNED,
        BYTES_SPILLED_TO_LOCAL_STORAGE,
        BYTES_SPILLED_TO_REMOTE_STORAGE,
        PARTITIONS_SCANNED,
        PARTITIONS_TOTAL,
        PERCENTAGE_SCANNED_FROM_CACHE,
        ROWS_PRODUCED,
        EXECUTION_STATUS,
        ERROR_CODE,
        ERROR_MESSAGE,
        CREDITS_USED_CLOUD_SERVICES
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        AND EXECUTION_STATUS = 'SUCCESS'
        AND QUERY_TYPE NOT IN ('SHOW', 'DESCRIBE', 'USE')
    ORDER BY START_TIME DESC
    """
    
    try:
        df = session.sql(query).to_pandas()
        df['TOTAL_ELAPSED_TIME_SEC'] = df['TOTAL_ELAPSED_TIME'] / 1000
        df['EXECUTION_TIME_SEC'] = df['EXECUTION_TIME'] / 1000
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

def analyze_cartesian_joins(df):
    """Identify potential cartesian joins based on query patterns"""
    issues = []
    
    for idx, row in df.iterrows():
        query_text = str(row['QUERY_TEXT']).upper()
        
        has_join = 'JOIN' in query_text
        has_on_or_using = 'ON' in query_text or 'USING' in query_text
        missing_join_condition = has_join and not has_on_or_using
        has_cross_join = 'CROSS JOIN' in query_text
        
        rows_produced = row['ROWS_PRODUCED'] if pd.notna(row['ROWS_PRODUCED']) else 0
        bytes_scanned = row['BYTES_SCANNED'] if pd.notna(row['BYTES_SCANNED']) else 1
        
        high_row_explosion = rows_produced > 1000000 and (rows_produced / max(bytes_scanned, 1)) > 10
        
        if missing_join_condition or has_cross_join or high_row_explosion:
            severity = 'CRITICAL' if missing_join_condition or has_cross_join else 'HIGH'
            reason = []
            
            if missing_join_condition:
                reason.append("Missing ON/USING clause in JOIN")
            if has_cross_join:
                reason.append("Explicit CROSS JOIN detected")
            if high_row_explosion:
                reason.append("Row explosion detected (high rows produced vs bytes scanned)")
            
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'ROWS_PRODUCED': rows_produced,
                'SEVERITY': severity,
                'ISSUE': 'Potential Cartesian Join',
                'REASON': ' | '.join(reason),
                'RECOMMENDATION': 'Add explicit JOIN conditions with ON or USING clause. Avoid CROSS JOINs unless necessary. Consider using range join optimization or ASOF joins for time-series data.'
            })
    
    return pd.DataFrame(issues)

def analyze_spilling(df):
    """Identify queries with memory spilling"""
    issues = []
    
    for idx, row in df.iterrows():
        local_spill = row['BYTES_SPILLED_TO_LOCAL_STORAGE'] if pd.notna(row['BYTES_SPILLED_TO_LOCAL_STORAGE']) else 0
        remote_spill = row['BYTES_SPILLED_TO_REMOTE_STORAGE'] if pd.notna(row['BYTES_SPILLED_TO_REMOTE_STORAGE']) else 0
        
        if local_spill > 0 or remote_spill > 0:
            severity = 'CRITICAL' if remote_spill > 0 else 'MEDIUM'
            total_spill = local_spill + remote_spill
            
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'WAREHOUSE_SIZE': row['WAREHOUSE_SIZE'],
                'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                'LOCAL_SPILL_GB': round(local_spill / (1024**3), 2),
                'REMOTE_SPILL_GB': round(remote_spill / (1024**3), 2),
                'SEVERITY': severity,
                'ISSUE': 'Memory Spilling',
                'RECOMMENDATION': f'Increase warehouse size to provide more memory. Current size: {row["WAREHOUSE_SIZE"]}. Consider upgrading to next tier or optimizing query with filters, CTEs, or temp tables to reduce intermediate data size.'
            })
    
    return pd.DataFrame(issues)

def analyze_warehouse_sizing(df):
    """Identify inefficient warehouse usage"""
    issues = []
    
    grouped = df.groupby(['WAREHOUSE_NAME', 'WAREHOUSE_SIZE']).agg({
        'EXECUTION_TIME_SEC': ['mean', 'max', 'count'],
        'QUEUED_OVERLOAD_TIME': 'sum'
    }).reset_index()
    
    for idx, row in grouped.iterrows():
        warehouse = row['WAREHOUSE_NAME']
        size = row['WAREHOUSE_SIZE']
        avg_exec = row[('EXECUTION_TIME_SEC', 'mean')]
        max_exec = row[('EXECUTION_TIME_SEC', 'max')]
        query_count = row[('EXECUTION_TIME_SEC', 'count')]
        queued_time = row[('QUEUED_OVERLOAD_TIME', 'sum')]
        
        if avg_exec < 5 and size in ['LARGE', 'X-LARGE', '2X-LARGE', '3X-LARGE', '4X-LARGE']:
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'AVG_EXEC_TIME_SEC': round(avg_exec, 2),
                'QUERY_COUNT': query_count,
                'SEVERITY': 'MEDIUM',
                'ISSUE': 'Oversized Warehouse',
                'RECOMMENDATION': f'Warehouse "{warehouse}" has fast queries (avg {avg_exec:.1f}s) but is sized as {size}. Consider downsizing to SMALL or MEDIUM to save credits without performance loss.'
            })
        
        if queued_time > 30000:
            issues.append({
                'WAREHOUSE': warehouse,
                'SIZE': size,
                'QUEUED_TIME_SEC': round(queued_time / 1000, 2),
                'QUERY_COUNT': query_count,
                'SEVERITY': 'HIGH',
                'ISSUE': 'Warehouse Queuing',
                'RECOMMENDATION': f'Warehouse "{warehouse}" has significant queuing ({queued_time/1000:.1f}s total). Consider scaling up or enabling multi-cluster mode for better concurrency.'
            })
    
    return pd.DataFrame(issues)

def analyze_poor_pruning(df):
    """Identify queries with poor partition pruning"""
    issues = []
    
    for idx, row in df.iterrows():
        partitions_scanned = row['PARTITIONS_SCANNED'] if pd.notna(row['PARTITIONS_SCANNED']) else 0
        partitions_total = row['PARTITIONS_TOTAL'] if pd.notna(row['PARTITIONS_TOTAL']) else 0
        
        if partitions_total > 0:
            scan_percentage = (partitions_scanned / partitions_total) * 100
            
            if scan_percentage > 50 and partitions_total > 100:
                issues.append({
                    'QUERY_ID': row['QUERY_ID'],
                    'USER_NAME': row['USER_NAME'],
                    'WAREHOUSE': row['WAREHOUSE_NAME'],
                    'EXECUTION_TIME_SEC': row['EXECUTION_TIME_SEC'],
                    'PARTITIONS_SCANNED': partitions_scanned,
                    'PARTITIONS_TOTAL': partitions_total,
                    'SCAN_PERCENTAGE': round(scan_percentage, 1),
                    'SEVERITY': 'MEDIUM',
                    'ISSUE': 'Poor Partition Pruning',
                    'RECOMMENDATION': f'Query scans {scan_percentage:.1f}% of partitions. Add WHERE clause filters on clustered columns, or define clustering keys on frequently filtered columns to improve pruning.'
                })
    
    return pd.DataFrame(issues)

def analyze_cache_efficiency(df):
    """Identify queries with low cache usage"""
    issues = []
    
    for idx, row in df.iterrows():
        cache_percentage = row['PERCENTAGE_SCANNED_FROM_CACHE'] if pd.notna(row['PERCENTAGE_SCANNED_FROM_CACHE']) else 0
        execution_time = row['EXECUTION_TIME_SEC']
        
        if cache_percentage < 20 and execution_time > 10:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'WAREHOUSE': row['WAREHOUSE_NAME'],
                'EXECUTION_TIME_SEC': execution_time,
                'CACHE_PERCENTAGE': round(cache_percentage, 1),
                'SEVERITY': 'LOW',
                'ISSUE': 'Low Cache Usage',
                'RECOMMENDATION': f'Only {cache_percentage:.1f}% cached. Consider: 1) Keeping warehouse active longer (reduce auto-suspend), 2) Result caching for repeated queries, 3) Scheduling similar queries on same warehouse.'
            })
    
    return pd.DataFrame(issues)

def analyze_long_compilation(df):
    """Identify queries with excessive compilation time"""
    issues = []
    
    for idx, row in df.iterrows():
        compilation_time = row['COMPILATION_TIME'] if pd.notna(row['COMPILATION_TIME']) else 0
        execution_time = row['EXECUTION_TIME'] if pd.notna(row['EXECUTION_TIME']) else 1
        total_time = row['TOTAL_ELAPSED_TIME'] if pd.notna(row['TOTAL_ELAPSED_TIME']) else 1
        
        compilation_pct = (compilation_time / max(total_time, 1)) * 100
        
        if compilation_pct > 30 and compilation_time > 5000:
            issues.append({
                'QUERY_ID': row['QUERY_ID'],
                'USER_NAME': row['USER_NAME'],
                'COMPILATION_TIME_SEC': round(compilation_time / 1000, 2),
                'COMPILATION_PCT': round(compilation_pct, 1),
                'SEVERITY': 'MEDIUM',
                'ISSUE': 'Long Compilation Time',
                'RECOMMENDATION': f'Compilation takes {compilation_pct:.1f}% of total time. Simplify complex queries, reduce number of CTEs, avoid dynamic SQL, or break into smaller queries.'
            })
    
    return pd.DataFrame(issues)

df = load_query_history()
warehouse_df = load_warehouse_metering()

if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Queries (24h)", len(df))
    with col2:
        avg_time = df['EXECUTION_TIME_SEC'].mean()
        st.metric("Avg Execution Time", f"{avg_time:.2f}s")
    with col3:
        total_bytes = df['BYTES_SCANNED'].sum() / (1024**4)
        st.metric("Total Data Scanned", f"{total_bytes:.2f} TB")
    with col4:
        if not warehouse_df.empty:
            total_credits = warehouse_df['CREDITS_USED'].sum()
            st.metric("Total Credits Used", f"{total_credits:.2f}")
        else:
            st.metric("Total Credits Used", "N/A")
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Issues Analysis", "⚠️ Top Offenders", "📈 Trends"])
    
    with tab1:
        st.subheader("Credit Usage by Warehouse")
        
        if not warehouse_df.empty:
            warehouse_summary = warehouse_df.groupby('WAREHOUSE_NAME').agg({
                'CREDITS_USED': 'sum',
                'CREDITS_USED_COMPUTE': 'sum',
                'CREDITS_USED_CLOUD_SERVICES': 'sum'
            }).reset_index().sort_values('CREDITS_USED', ascending=False)
            
            fig = px.bar(
                warehouse_summary.head(10),
                x='WAREHOUSE_NAME',
                y='CREDITS_USED',
                title='Top 10 Warehouses by Credit Usage (24h)',
                labels={'CREDITS_USED': 'Credits Used', 'WAREHOUSE_NAME': 'Warehouse'}
            )
            st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Query Distribution by Type")
            query_type_dist = df['QUERY_TYPE'].value_counts().head(10)
            fig = px.pie(
                values=query_type_dist.values,
                names=query_type_dist.index,
                title='Query Types'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Top Users by Query Count")
            user_dist = df['USER_NAME'].value_counts().head(10)
            fig = px.bar(
                x=user_dist.index,
                y=user_dist.values,
                labels={'x': 'User', 'y': 'Query Count'},
                title='Most Active Users'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("🔍 Detailed Issues Analysis")
        
        cartesian_issues = analyze_cartesian_joins(df)
        spilling_issues = analyze_spilling(df)
        warehouse_issues = analyze_warehouse_sizing(df)
        pruning_issues = analyze_poor_pruning(df)
        cache_issues = analyze_cache_efficiency(df)
        compilation_issues = analyze_long_compilation(df)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Cartesian Join Issues", len(cartesian_issues), delta=None, delta_color="inverse")
        with col2:
            st.metric("Spilling Issues", len(spilling_issues), delta=None, delta_color="inverse")
        with col3:
            st.metric("Warehouse Sizing Issues", len(warehouse_issues), delta=None, delta_color="inverse")
        
        st.markdown("---")
        
        if not cartesian_issues.empty:
            st.subheader("🔴 Potential Cartesian Joins")
            st.dataframe(
                cartesian_issues[['QUERY_ID', 'USER_NAME', 'WAREHOUSE', 'EXECUTION_TIME_SEC', 'ROWS_PRODUCED', 'SEVERITY', 'REASON', 'RECOMMENDATION']],
                use_container_width=True
            )
        else:
            st.success("✅ No cartesian join issues detected")
        
        st.markdown("---")
        
        if not spilling_issues.empty:
            st.subheader("🟠 Memory Spilling Issues")
            st.dataframe(
                spilling_issues[['QUERY_ID', 'USER_NAME', 'WAREHOUSE', 'WAREHOUSE_SIZE', 'LOCAL_SPILL_GB', 'REMOTE_SPILL_GB', 'SEVERITY', 'RECOMMENDATION']],
                use_container_width=True
            )
        else:
            st.success("✅ No spilling issues detected")
        
        st.markdown("---")
        
        if not warehouse_issues.empty:
            st.subheader("🟡 Warehouse Sizing Issues")
            st.dataframe(
                warehouse_issues,
                use_container_width=True
            )
        else:
            st.success("✅ No warehouse sizing issues detected")
        
        st.markdown("---")
        
        if not pruning_issues.empty:
            st.subheader("🔵 Poor Partition Pruning")
            st.dataframe(
                pruning_issues[['QUERY_ID', 'USER_NAME', 'PARTITIONS_SCANNED', 'PARTITIONS_TOTAL', 'SCAN_PERCENTAGE', 'RECOMMENDATION']],
                use_container_width=True
            )
        else:
            st.success("✅ No significant pruning issues detected")
        
        st.markdown("---")
        
        if not compilation_issues.empty:
            st.subheader("⚪ Long Compilation Times")
            st.dataframe(
                compilation_issues[['QUERY_ID', 'USER_NAME', 'COMPILATION_TIME_SEC', 'COMPILATION_PCT', 'RECOMMENDATION']],
                use_container_width=True
            )
        else:
            st.success("✅ No compilation issues detected")
        
        st.markdown("---")
        
        if not cache_issues.empty:
            st.subheader("🟣 Low Cache Efficiency")
            st.dataframe(
                cache_issues[['QUERY_ID', 'USER_NAME', 'WAREHOUSE', 'EXECUTION_TIME_SEC', 'CACHE_PERCENTAGE', 'RECOMMENDATION']],
                use_container_width=True
            )
        else:
            st.success("✅ No cache efficiency issues detected")
    
    with tab3:
        st.subheader("⚠️ Top 20 Most Expensive Queries")
        
        top_queries = df.nlargest(20, 'EXECUTION_TIME_SEC')[
            ['QUERY_ID', 'USER_NAME', 'WAREHOUSE_NAME', 'WAREHOUSE_SIZE', 'EXECUTION_TIME_SEC', 'BYTES_SCANNED', 'ROWS_PRODUCED']
        ].copy()
        
        top_queries['BYTES_SCANNED_GB'] = (top_queries['BYTES_SCANNED'] / (1024**3)).round(2)
        top_queries = top_queries.drop('BYTES_SCANNED', axis=1)
        
        st.dataframe(top_queries, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Execution Time Distribution")
        
        fig = px.histogram(
            df,
            x='EXECUTION_TIME_SEC',
            nbins=50,
            title='Query Execution Time Distribution',
            labels={'EXECUTION_TIME_SEC': 'Execution Time (seconds)'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("📈 Query Volume Over Time")
        
        df['HOUR'] = pd.to_datetime(df['START_TIME']).dt.floor('H')
        hourly_queries = df.groupby('HOUR').size().reset_index(name='QUERY_COUNT')
        
        fig = px.line(
            hourly_queries,
            x='HOUR',
            y='QUERY_COUNT',
            title='Queries per Hour (Last 24 Hours)',
            labels={'HOUR': 'Time', 'QUERY_COUNT': 'Number of Queries'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("📊 Execution Time Trend")
        
        hourly_exec = df.groupby('HOUR')['EXECUTION_TIME_SEC'].mean().reset_index()
        
        fig = px.line(
            hourly_exec,
            x='HOUR',
            y='EXECUTION_TIME_SEC',
            title='Average Execution Time per Hour',
            labels={'HOUR': 'Time', 'EXECUTION_TIME_SEC': 'Avg Execution Time (seconds)'}
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

    st.markdown("---")
    st.subheader("💡 Key Recommendations Summary")
    
    all_issues = []
    
    if not cartesian_issues.empty:
        all_issues.append(f"🔴 **{len(cartesian_issues)} Cartesian Join Issues**: Review JOIN conditions and add explicit ON clauses")
    
    if not spilling_issues.empty:
        critical_spills = len(spilling_issues[spilling_issues['SEVERITY'] == 'CRITICAL'])
        all_issues.append(f"🟠 **{len(spilling_issues)} Spilling Issues** ({critical_spills} critical): Increase warehouse sizes or optimize queries")
    
    if not warehouse_issues.empty:
        all_issues.append(f"🟡 **{len(warehouse_issues)} Warehouse Issues**: Adjust warehouse sizes based on workload patterns")
    
    if not pruning_issues.empty:
        all_issues.append(f"🔵 **{len(pruning_issues)} Pruning Issues**: Add clustering keys or improve WHERE clause filters")
    
    if not cache_issues.empty:
        all_issues.append(f"🟣 **{len(cache_issues)} Cache Efficiency Issues**: Adjust auto-suspend settings or consolidate queries")
    
    if not compilation_issues.empty:
        all_issues.append(f"⚪ **{len(compilation_issues)} Compilation Issues**: Simplify complex queries or break them down")
    
    if all_issues:
        for issue in all_issues:
            st.markdown(issue)
    else:
        st.success("✅ No major issues detected! Your queries are running efficiently.")
    
    st.markdown("---")
    st.info("""
    **Best Practices for Credit Optimization:**
    - ✅ Set auto-suspend to 1-5 minutes for warehouses
    - ✅ Start with X-Small/Small warehouses and scale based on performance
    - ✅ Separate workloads into dedicated warehouses (ETL, BI, Analytics)
    - ✅ Use clustering keys on frequently filtered columns
    - ✅ Avoid SELECT * and scan only necessary columns
    - ✅ Use result caching for repeated queries
    - ✅ Monitor and review query patterns regularly
    - ✅ Set statement timeouts to prevent runaway queries
    """)

else:
    st.warning("No query history data available for the past 24 hours. Please check your permissions or try again later.")
    st.info("**Required Permissions:** Access to SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (typically requires ACCOUNTADMIN role or IMPORTED PRIVILEGES on SNOWFLAKE database)")
