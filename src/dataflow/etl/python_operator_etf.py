
# 匯入 Airflow 所需元件
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 將專案路徑加入 sys.path
sys.path.append('/opt/projects/web_crawler_etf')

# 匯入兩個 main 函式
from crawler.producer_main_tw import main as run_producer_tw
from crawler.producer_main_us import main as run_producer_us

# 回傳 TW 的 PythonOperator 任務
def create_producer_task_tw() -> PythonOperator:
    return PythonOperator(
        task_id="producer_main_tw",
        python_callable=run_producer_tw,
        queue="twse",
    )

# 回傳 US 的 PythonOperator 任務
def create_producer_task_us() -> PythonOperator:
    return PythonOperator(
        task_id="producer_main_us",
        python_callable=run_producer_us,
        queue="tpex",
    )

# 建立 DAG
with DAG(
    dag_id='etl_producer_dag',
    description='Run TW and US ETF producers',
    schedule_interval='@daily',  # 或根據實際需求調整
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'crawler']
) as dag:

    # 建立任務
    task_tw = create_producer_task_tw()
    task_us = create_producer_task_us()

    # 任務依賴關係（可選）
    task_tw >> task_us  # 也可以改成平行執行：無需設定依賴
