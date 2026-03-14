#!/bin/bash

# --- 1. 自动获取当前目录，不再写死 vickyqi ---
PROJECT_ROOT=$(pwd)

# --- 2. 声明环境 (使用 $HOME 适配所有人) ---
export SPARK_HOME=$HOME/spark-3.4.1-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/default-java

clear
echo "🚀 [TEAM 12] STARTING FULL AUTOMATED PIPELINE..."

# --- 3. 交互式输入 (解决老师没有你密码的问题) ---
# 如果环境里没有配置密码，就询问用户
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

# --- 4. 运行 Step 1: Spark (使用相对路径) ---
echo "Step 1: Running PySpark Data Cleaning..."
# 确保这里指向你存放 run_pipeline.sh 的正确相对位
bash ~/nyc-311-pipeline/spark/run_pipeline.sh
# --- 5. 检查原子性 ---
if [ $? -eq 0 ]; then
    echo "✅ Step 1 Success!"
else
    echo "❌ Step 1 Failed. Pipeline stopped."
    exit 1
fi

# --- 6. 运行 Step 2: Snowflake ---
echo "Step 2: Loading to Snowflake..."
# 不再使用 -c my_pipeline，直接使用上面输入的变量
snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$PROJECT_ROOT/snowflake/405_Final_Project.sql"
