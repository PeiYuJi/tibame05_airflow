# 匯入 Airflow 核心模組
import airflow

# 匯入自定義的常數設定，用於統一管理 DAG 參數與執行限制
from dataflow.constant import (
    # 預設參數，例如 owner、start_date、retries 等
    DEFAULT_ARGS,
    # 限制同一時間同一 DAG 最多允許幾個執行實例
    MAX_ACTIVE_RUNS,
)

# 匯入自定義的 DockerOperator 任務建立函式

from dataflow.etl.producer_etf import (
    create_producer_task_tw,
    create_producer_task_us,
)

from datetime import datetime

# 定義 DAG，並用 with 語法將任務放入 DAG 環境中

# 台股：每天 17:30 更新
with airflow.DAG(
    # DAG 的唯一名稱，用來識別 DAG
    dag_id="ProducerScheduler_TWSE",
    # 套用預設參數設定
    default_args=DEFAULT_ARGS,
    # 每天 17:30 執行
    schedule_interval="30 17 * * *", 
    # 限制同時執行的最大 DAG 實例數
    max_active_runs=MAX_ACTIVE_RUNS,
    # 禁止補跑過去未執行的排程
    catchup=False,
    
    start_date=datetime(2025, 9, 15),
) as dag_tw:
    create_producer_task_tw()  # 傳遞 market 參數給 task


# 美股：每天 07:00 更新
with airflow.DAG(
    dag_id="ProducerScheduler_US",
    default_args=DEFAULT_ARGS,
    # 美股：每天 07:00 更新
    schedule_interval="0 7 * * *",
    max_active_runs=MAX_ACTIVE_RUNS,
    catchup=False,
    start_date=datetime(2025, 9, 15),
) as dag_us:
    create_producer_task_us()