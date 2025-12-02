# Quick Deployment Guide

## 🚀 5-Minute Setup

### Step 1: Prepare Snowflake Environment
Run these SQL commands in Snowsight:

```sql
-- Switch to admin role
USE ROLE ACCOUNTADMIN;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS STREAMLIT_DB;
CREATE SCHEMA IF NOT EXISTS STREAMLIT_DB.CREDIT_ANALYZER;

-- Create warehouse for Streamlit
CREATE WAREHOUSE IF NOT EXISTS STREAMLIT_WH 
    WITH WAREHOUSE_SIZE='X-SMALL' 
    AUTO_SUSPEND=60 
    AUTO_RESUME=TRUE
    INITIALLY_SUSPENDED=TRUE;

-- Verify access to required views
SELECT COUNT(*) 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

SELECT COUNT(*) 
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
```

### Step 2: Create Streamlit App in Snowsight

1. **Navigate**: Projects → Streamlit → + Streamlit App

2. **Configure**:
   - **App name**: `Credit Usage Analyzer`
   - **Warehouse**: `STREAMLIT_WH`
   - **Location**: `STREAMLIT_DB.CREDIT_ANALYZER`

3. **Deploy**:
   - Delete default code
   - Copy entire contents of `streamlit_app.py`
   - Paste into editor
   - Click **Run**

### Step 3: Wait & Verify

- Initial load: 30-60 seconds
- You should see the dashboard with credit usage metrics
- If you see "No query history data", check permissions or wait for ACCOUNT_USAGE latency (up to 45 min)

## ✅ Success Checklist

- [ ] ACCOUNTADMIN role active
- [ ] Anaconda terms accepted (Admin → Billing & Terms)
- [ ] Database and warehouse created
- [ ] Streamlit app created and running
- [ ] Dashboard displays data (or shows permission instructions)

## 📝 Quick Reference

### Required Privileges
```sql
-- Grant to other roles
GRANT USAGE ON DATABASE STREAMLIT_DB TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA STREAMLIT_DB.CREDIT_ANALYZER TO ROLE DATA_ENGINEER;
GRANT USAGE ON STREAMLIT STREAMLIT_DB.CREDIT_ANALYZER."Credit Usage Analyzer" TO ROLE DATA_ENGINEER;
```

### Share App with Team
Use the **Share** button in Streamlit editor, or:

```sql
GRANT USAGE ON STREAMLIT STREAMLIT_DB.CREDIT_ANALYZER."Credit Usage Analyzer" TO ROLE YOUR_TEAM_ROLE;
```

## 🔧 Troubleshooting

| Issue | Solution |
|-------|----------|
| "Access denied" error | Use ACCOUNTADMIN role or grant IMPORTED PRIVILEGES on SNOWFLAKE database |
| No data showing | Wait 45 minutes for ACCOUNT_USAGE latency, or ensure queries ran in last 24h |
| App won't run | Verify Anaconda terms accepted in Admin → Billing & Terms |
| Slow performance | Increase STREAMLIT_WH to SMALL or reduce time window in code |

## 📞 Need Help?

1. Check app logs in Streamlit editor
2. Verify permissions with SQL queries above
3. Review ACCOUNT_USAGE documentation
4. Contact Snowflake support for account-specific issues

---

**Next Steps**: Once running, review the **Issues Analysis** tab for immediate optimization opportunities!
