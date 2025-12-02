# ⚠️ IMPORTANT: This is NOT a Local Application

## What is This Project?

This project contains **deployment code for Snowflake Snowsight**, not a locally-runnable application.

Think of this like:
- AWS CloudFormation templates (deployment config, not runnable app)
- Kubernetes manifests (deployment config, not runnable app)
- **Snowflake Streamlit code (deployment config, not runnable app)**

## Why Can't This Run Locally?

The code in `streamlit_app.py` uses Snowflake-specific features that **ONLY work inside Snowsight**:

```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()  # ❌ This ONLY works in Snowsight
```

This function:
- Only exists in Snowflake's managed Streamlit environment
- Automatically provides authenticated access to your Snowflake account
- Cannot be simulated or run outside of Snowsight

Additionally, the app queries:
```sql
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY  -- ❌ Only exists in Snowflake
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY  -- ❌ Only exists in Snowflake
```

These views:
- Are Snowflake system tables
- Require ACCOUNTADMIN privileges
- Do not exist in any local database

## How to Use This Project

### Step 1: Understand What You Have
You have **source code** that needs to be deployed to Snowflake Snowsight.

### Step 2: Deploy to Snowsight
Follow these steps:

1. **Run Setup SQL**
   - Open `SNOWSIGHT_SETUP.sql`
   - Run it in Snowsight SQL Worksheet
   - This creates the database, schema, and warehouse

2. **Create Streamlit App in Snowsight**
   - Go to app.snowflake.com
   - Navigate to: Projects → Streamlit
   - Click: "+ Streamlit App"
   - Configure:
     - Name: `Credit Usage Analyzer`
     - Warehouse: `STREAMLIT_WH`
     - Location: `STREAMLIT_DB.CREDIT_ANALYZER`

3. **Deploy the Code**
   - Open `streamlit_app.py` from this project
   - Copy ALL the code
   - Paste into Snowsight's Streamlit editor
   - Click "Run"

4. **Use the Dashboard**
   - The app will load in Snowsight
   - You'll see credit usage analysis
   - Share with your team using the Share button

### Step 3: Read the Documentation
- `DEPLOYMENT_GUIDE.md` - Quick setup instructions
- `README.md` - Full documentation
- `SNOWSIGHT_SETUP.sql` - Database setup script

## What About Replit?

**Replit is just the development environment** where this code was created. 

Think of it like:
- Using VS Code to write Snowflake SQL → You still run the SQL in Snowflake
- Using GitHub to store CloudFormation templates → You still deploy to AWS
- Using Replit to create Streamlit code → **You still deploy to Snowsight**

## Quick Comparison

| Feature | Local Streamlit | Snowsight Streamlit |
|---------|----------------|---------------------|
| Where it runs | Your computer | Snowflake cloud |
| Data access | Need credentials | Automatic via `get_active_session()` |
| Authentication | Manual setup | Handled by Snowflake |
| Deployment | `streamlit run app.py` | Copy code to Snowsight editor |
| **This project** | ❌ Not supported | ✅ **This is what you need** |

## TL;DR

1. ✅ This is Snowsight deployment code
2. ❌ This cannot run locally or in Replit
3. 📋 Follow `DEPLOYMENT_GUIDE.md` to deploy to Snowsight
4. 🎯 The app analyzes Snowflake credit usage from inside Snowflake

---

**Next Step**: Open Snowsight and follow the deployment guide!
