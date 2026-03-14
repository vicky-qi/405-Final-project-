#!/bin/bash

# --- 1. 自动获取当前目录，不再写死 ---
PROJECT_ROOT=$(pwd)

# --- 2. 声明环境 ---
export SPARK_HOME=$HOME/spark-3.4.1-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/default-java

clear
echo "🚀 [TEAM 12] STARTING FULL AUTOMATED PIPELINE..."

# --- 3. 交互式输入 ---
if [ -z "$SNOWSQL_PWD" ]; then
    echo "🔒 Security: Please provide your Snowflake credentials."
    read -p "Snowflake Account (e.g. xy12345.us-east-1): " SNOW_ACCT
    read -p "Snowflake User: " SNOW_USER
    read -s -p "Snowflake Password: " SNOW_PASS
    echo ""
    export SNOWSQL_ACCOUNT=$SNOW_ACCT
    export SNOWSQL_USER=$SNOW_USER
    export SNOWSQL_PWD=$SNOW_PASS
fi

# --- 4. 运行 Step 1: Spark (完美适配相对路径) ---
echo "Step 1: Running PySpark Data Cleaning..."
spark-submit spark/spark-job.py

# --- 5. 检查原子性 ---
if [ $? -eq 0 ]; then
    echo "✅ Step 1 Success!"
else
    echo "❌ Step 1 Failed. Pipeline stopped."
    exit 1
fi

# --- 6. 运行 Step 2: Snowflake (完美适配相对路径) ---
echo "Step 2: Loading to Snowflake..."
snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$PROJECT_ROOT/snowflake/405_Final_Project.sql"
