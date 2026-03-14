#!/bin/bash
echo "🛠️ [TEAM 12] STARTING ENVIRONMENT SETUP..."

# 1. 更新系统并安装 Java (Spark 必需)
sudo apt-get update
sudo apt-get install -y default-jdk unzip wget

# 2. 安装 Spark 3.4.1
# 改用 $HOME 确保判断精准
if [ ! -d "$HOME/spark-3.4.1-bin-hadoop3" ]; then
    echo "📥 Downloading Spark..."
    cd $HOME
    wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
    tar -xvzf spark-3.4.1-bin-hadoop3.tgz
    rm spark-3.4.1-bin-hadoop3.tgz  # 装完删掉压缩包，节省空间
fi

# 3. 安装 SnowSQL
if ! command -v snowsql &> /dev/null; then
    echo "📥 Installing SnowSQL..."
    cd $HOME
    curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.31-linux_x86_64.bash
    # 使用 -b 参数指定安装到 bin 目录
    bash snowsql-1.2.31-linux_x86_64.bash -b $HOME/bin
fi

echo "✅ [SUCCESS] All dependencies installed!"
echo "💡 Instructions: Now you can run './pipeline.sh' to start the process."


