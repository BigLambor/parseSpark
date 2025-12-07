#!/bin/bash
# Spark EventLog解析器提交脚本 (PySpark版本)
# 用法:
#   ./submit_parser.sh <cluster_name> [target_date]
# 示例:
#   ./submit_parser.sh cluster1              # 解析昨天的日志
#   ./submit_parser.sh cluster1 2025-12-05   # 解析指定日期

set -e  # 遇到错误立即退出

# 参数检查
if [ $# -lt 1 ]; then
    echo "错误: 缺少必需参数"
    echo "用法: $0 <cluster_name> [target_date]"
    echo "示例: $0 cluster1 2025-12-05"
    exit 1
fi

CLUSTER_NAME=$1
TARGET_DATE=${2:-$(date -d "yesterday" +%Y-%m-%d)}

# 配置变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAIN_SCRIPT="${SCRIPT_DIR}/parse_spark_logs.py"
CONFIG_PATH="${SCRIPT_DIR}/config.yaml"
SPARK_HOME=${SPARK_HOME:-/usr/bch/3.3.0/spark} # need
HIVE_CONF_DIR=${HIVE_CONF_DIR:-/usr/bch/3.3.0/hive/conf} # need 
HIVE_SITE_XML="${HIVE_CONF_DIR}/hive-site.xml"

# Spark资源配置（可通过环境变量覆盖）
DRIVER_MEMORY=${DRIVER_MEMORY:-8g}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-12g}
EXECUTOR_MEMORY_OVERHEAD=${EXECUTOR_MEMORY_OVERHEAD:-2g}
EXECUTOR_CORES=${EXECUTOR_CORES:-4}
NUM_EXECUTORS=${NUM_EXECUTORS:-200}
PARALLELISM=${PARALLELISM:-2000}

# 应用名称
APP_NAME="SparkEventLogParser-${CLUSTER_NAME}-${TARGET_DATE}"

echo "=========================================="
echo "Spark EventLog解析任务提交 (PySpark)"
echo "=========================================="
echo "集群名称: $CLUSTER_NAME"
echo "目标日期: $TARGET_DATE"
echo "主程序: $MAIN_SCRIPT"
echo "配置文件: $CONFIG_PATH"
echo "=========================================="
echo "资源配置:"
echo "  Driver内存: $DRIVER_MEMORY"
echo "  Executor内存: $EXECUTOR_MEMORY"
echo "  Executor核心数: $EXECUTOR_CORES"
echo "  Executor数量: $NUM_EXECUTORS"
echo "  并行度: $PARALLELISM"
echo "  Hive配置目录: $HIVE_CONF_DIR"
echo "=========================================="

# 检查主程序是否存在
if [ ! -f "$MAIN_SCRIPT" ]; then
    echo "错误: 主程序文件不存在: $MAIN_SCRIPT"
    exit 1
fi

# 检查配置文件是否存在
if [ ! -f "$CONFIG_PATH" ]; then
    echo "错误: 配置文件不存在: $CONFIG_PATH"
    echo "请先创建配置文件: cp config.yaml.example config.yaml"
    exit 1
fi

# 检查Hive配置文件
if [ ! -f "$HIVE_SITE_XML" ]; then
    echo "警告: Hive配置文件不存在: $HIVE_SITE_XML"
    echo "尝试从常见位置查找..."
    for hive_conf in "/etc/hive/conf/hive-site.xml" "/usr/bch/current/hive/conf/hive-site.xml" "$SPARK_HOME/conf/hive-site.xml"; do
        if [ -f "$hive_conf" ]; then
            HIVE_SITE_XML="$hive_conf"
            echo "找到Hive配置: $HIVE_SITE_XML"
            break
        fi
    done
    if [ ! -f "$HIVE_SITE_XML" ]; then
        echo "警告: 无法找到hive-site.xml，可能会导致Hive连接失败"
        HIVE_SITE_XML=""
    fi
fi

# 打包Python模块
echo "打包Python模块..."
cd ${SCRIPT_DIR}

# 清理旧的zip文件
rm -f parser.zip models.zip utils.zip

# 打包模块
zip -q -r parser.zip parser/ -x "*.pyc" -x "*__pycache__*" -x "*.git*"
zip -q -r models.zip models/ -x "*.pyc" -x "*__pycache__*" -x "*.git*"
zip -q -r utils.zip utils/ -x "*.pyc" -x "*__pycache__*" -x "*.git*"

echo "Python模块打包完成"

# 上传配置文件到HDFS（如果需要）
HDFS_CONFIG_PATH="hdfs:///tmp/spark-parser/config_${CLUSTER_NAME}.yaml"
echo "上传配置文件到HDFS: $HDFS_CONFIG_PATH"
hadoop fs -mkdir -p /tmp/spark-parser || true
hadoop fs -put -f "$CONFIG_PATH" "$HDFS_CONFIG_PATH" || {
    echo "警告: 无法上传配置到HDFS，将使用本地配置"
    HDFS_CONFIG_PATH="$CONFIG_PATH"
}

# 构建额外的文件列表（hive-site.xml）
EXTRA_FILES=""
if [ -n "$HIVE_SITE_XML" ] && [ -f "$HIVE_SITE_XML" ]; then
    EXTRA_FILES="--files $HIVE_SITE_XML"
    echo "将分发Hive配置文件: $HIVE_SITE_XML"
fi

# 提交Spark任务
echo "提交Spark任务..."
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "$APP_NAME" \
  --driver-memory $DRIVER_MEMORY \
  --executor-memory $EXECUTOR_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --num-executors $NUM_EXECUTORS \
  $EXTRA_FILES \
  --conf spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD \
  --conf spark.sql.shuffle.partitions=$PARALLELISM \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.hadoop.hive.metastore.client.connect.retry.delay=5 \
  --conf spark.hadoop.hive.metastore.client.socket.timeout=1800 \
  --conf spark.app.cluster_name=$CLUSTER_NAME \
  --conf spark.app.target_date=$TARGET_DATE \
  --conf spark.app.config_path=$HDFS_CONFIG_PATH \
  --conf spark.app.skip_inprogress=true \
  --conf spark.app.parse_tasks=false \
  --py-files ${SCRIPT_DIR}/parser.zip,${SCRIPT_DIR}/models.zip,${SCRIPT_DIR}/utils.zip \
  $MAIN_SCRIPT

EXIT_CODE=$?

# 清理临时文件
cd ${SCRIPT_DIR}
rm -f parser.zip models.zip utils.zip

if [ $EXIT_CODE -eq 0 ]; then
    echo "=========================================="
    echo "任务提交成功！"
    echo "=========================================="
    echo "可以通过以下方式查看任务状态:"
    echo "  Spark UI: http://your-spark-history-server:18080"
    echo "  YARN UI: http://your-yarn-rm:8088"
    echo ""
    echo "查询解析结果:"
    echo "  hive -e \"SELECT COUNT(*) FROM meta.spark_applications WHERE dt='$TARGET_DATE' AND cluster_name='$CLUSTER_NAME'\""
    echo ""
    echo "数据质量检查:"
    echo "  hive -e \"SELECT status, COUNT(*) FROM meta.spark_applications WHERE dt='$TARGET_DATE' AND cluster_name='$CLUSTER_NAME' GROUP BY status\""
else
    echo "=========================================="
    echo "任务提交失败！退出码: $EXIT_CODE"
    echo "=========================================="
    exit $EXIT_CODE
fi
