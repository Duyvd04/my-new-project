from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'ecom_medallion_full_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Ingest from MySQL (Incremental Load logic inside script)
   # Ví dụ sửa task ingest_mysql trong file DAG của bạn
    ingest_mysql = BashOperator(
    task_id='ingest_mysql',
    bash_command='export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::") && python3 /opt/airflow/spark_scripts/ingestion/mysql_to_bronze.py'
    )

    # Task 2: Streaming Clickstream (Giả lập chạy 1 phiên)
    ingest_stream = BashOperator(
        task_id='ingest_clickstream',
        bash_command='python3 /opt/airflow/spark_scripts/ingestion/stream_to_bronze.py'
    )

    # Task 3: Clean data to Silver
    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command='python3 /opt/airflow/spark_scripts/transformation/bronze_to_silver.py'
    )

    # Task 4: Analytics to Gold
    analytics_gold = BashOperator(
        task_id='analytics_gold',
        bash_command='python3 /opt/airflow/spark_scripts/analytics/master_gold.py'
    )

    [ingest_mysql, ingest_stream] >> transform_silver >> analytics_gold