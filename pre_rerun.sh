#!/bin/bash
# 解析重跑前置脚本：清理指定日期分区，然后调用 submit_parser.sh
# 用法:
#   ./pre_rerun.sh <cluster_name> [target_date]
# 示例:
#   ./pre_rerun.sh cluster1            # 默认昨天
#   ./pre_rerun.sh cluster1 2025-12-01 # 指定日期

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "用法: $0 <cluster_name> [target_date]"
  exit 1
fi

CLUSTER_NAME="$1"
# GNU date 在 mac 上可能不可用，如需兼容可传入日期或自行调整
TARGET_DATE="${2:-$(date -d "yesterday" +%Y-%m-%d)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUBMIT_SCRIPT="${SCRIPT_DIR}/submit_parser.sh"

# 允许通过环境变量覆盖库名和表名，默认与 config_loader / create_hive_tables 保持一致
HIVE_DB="${HIVE_DB:-meta}"
APP_TABLE="${APP_TABLE:-spark_applications}"
JOB_TABLE="${JOB_TABLE:-spark_jobs}"
STAGE_TABLE="${STAGE_TABLE:-spark_stages}"
EXECUTOR_TABLE="${EXECUTOR_TABLE:-spark_executors}"
SQL_TABLE="${SQL_TABLE:-spark_sql_executions}"
CONFIG_TABLE="${CONFIG_TABLE:-spark_configs}"
STATUS_TABLE="${STATUS_TABLE:-spark_parser_status}"

echo "=========================================="
echo "重跑前置清理"
echo "集群:       ${CLUSTER_NAME}"
echo "目标日期:   ${TARGET_DATE}"
echo "Hive库:     ${HIVE_DB}"
echo "=========================================="

drop_partition() {
  local table="$1"
  local full_table="${HIVE_DB}.${table}"
  echo "Dropping partition dt='${TARGET_DATE}' on ${full_table} ..."
  hive -e "ALTER TABLE ${full_table} DROP IF EXISTS PARTITION (dt='${TARGET_DATE}');" \
    || echo "警告: 删除分区 ${full_table} 失败，继续执行"
}

# 清理状态表（确保扫描阶段不跳过已处理文件）
drop_partition "${STATUS_TABLE}"

# 可选：清理业务数据分区，避免旧结果干扰
drop_partition "${APP_TABLE}"
drop_partition "${JOB_TABLE}"
drop_partition "${STAGE_TABLE}"
drop_partition "${EXECUTOR_TABLE}"
drop_partition "${SQL_TABLE}"
drop_partition "${CONFIG_TABLE}"

echo "分区清理完成，开始重新提交解析任务..."

if [ ! -x "${SUBMIT_SCRIPT}" ]; then
  echo "错误: 找不到 submit_parser.sh: ${SUBMIT_SCRIPT}"
  exit 1
fi

#exec "${SUBMIT_SCRIPT}" "${CLUSTER_NAME}" "${TARGET_DATE}"

