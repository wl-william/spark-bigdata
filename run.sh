#!/bin/bash

################################################################################
# Spark千问视频文本处理器 - 运行脚本
#
# 用法: ./run.sh <处理日期>
# 示例: ./run.sh 2025-12-01
################################################################################

set -e  # 遇到错误立即退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 打印函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查参数
if [ $# -ne 1 ]; then
    log_error "参数错误！"
    echo "用法: $0 <处理日期>"
    echo "示例: $0 2025-12-01"
    exit 1
fi

PROCESS_DATE=$1
log_info "处理日期: $PROCESS_DATE"

# 检查API密钥
if [ -z "$DASHSCOPE_API_KEY" ]; then
    log_error "DASHSCOPE_API_KEY 环境变量未设置"
    echo "请执行: export DASHSCOPE_API_KEY='your-api-key'"
    exit 1
fi
log_info "API密钥已配置"

# 配置参数
APP_NAME="QianwenVideoTextProcessor-${PROCESS_DATE}"
MAIN_CLASS="com.bigdata.spark.QianwenVideoTextProcessor"
JAR_FILE="target/spark-qianwen-processor-1.0.0.jar"

# Spark配置
MASTER="yarn"
DEPLOY_MODE="cluster"  # 可选: client 或 cluster
NUM_EXECUTORS=10
EXECUTOR_CORES=2
EXECUTOR_MEMORY="4G"
DRIVER_MEMORY="2G"
DRIVER_CORES=2

# 检查jar文件
if [ ! -f "$JAR_FILE" ]; then
    log_error "Jar文件不存在: $JAR_FILE"
    log_info "请先执行: mvn clean package"
    exit 1
fi
log_info "Jar文件: $JAR_FILE"

# 构建spark-submit命令
log_info "开始提交Spark任务..."

spark-submit \
  --class $MAIN_CLASS \
  --name "$APP_NAME" \
  --master $MASTER \
  --deploy-mode $DEPLOY_MODE \
  --num-executors $NUM_EXECUTORS \
  --executor-cores $EXECUTOR_CORES \
  --executor-memory $EXECUTOR_MEMORY \
  --driver-memory $DRIVER_MEMORY \
  --driver-cores $DRIVER_CORES \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.task.maxFailures=4 \
  --conf spark.speculation=false \
  --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -Dfile.encoding=UTF-8" \
  --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -Dfile.encoding=UTF-8" \
  --files src/main/resources/log4j.properties \
  $JAR_FILE \
  $PROCESS_DATE

SUBMIT_STATUS=$?

if [ $SUBMIT_STATUS -eq 0 ]; then
    log_info "Spark任务提交成功！"
    log_info "查看任务状态: yarn application -list"
    log_info "查看日志: yarn logs -applicationId <application_id>"
else
    log_error "Spark任务提交失败，退出码: $SUBMIT_STATUS"
    exit $SUBMIT_STATUS
fi
