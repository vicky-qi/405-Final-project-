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
# --- 4. 运行 Step 1: Spark ---
echo "Step 1: Running PySpark Data Cleaning..."
spark-submit \
  --master local[*] \
  --driver-memory 10g \
  --executor-memory 10g \
  spark/spark-job.py \
  "data/311_Service_Requests_from_2020_to_Present_20260218.csv" \
  "data/pluto_25v4.csv" \
  "output/"

# --- 5. 检查原子性 ---
if [ $? -eq 0 ]; then
    echo "✅ Step 1 Success!"
else
    echo "❌ Step 1 Failed. Pipeline stopped."
    exit 1
fi

# --- 6. 运行 Step 2: Snowflake ---
echo "Step 2: Loading to Snowflake..."

# 先把 PUT 命令写进临时文件
TMPFILE=$(mktemp /tmp/put_cmd_XXXX.sql)
echo "USE DATABASE NYC_311_DB;"                                                          > $TMPFILE
echo "USE SCHEMA PUBLIC;"                                                               >> $TMPFILE
echo "PUT file://$PROJECT_ROOT/output/*.parquet @GCP_PARQUET_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;" >> $TMPFILE

# 用 -f 执行（PUT 只能用 -f，不能用 -q）
snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$TMPFILE"
rm -f $TMPFILE

# 再跑建表 + COPY INTO
snowsql -a $SNOWSQL_ACCOUNT -u $SNOWSQL_USER -f "$PROJECT_ROOT/snowflake/405_Final_Project.sql"
