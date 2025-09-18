from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'dinesh',
    'depends_on_past': False,
    'retries': 0,
}

# IST → UTC conversion
# 11:00 IST → 05:30 UTC
# 11:15 IST → 05:45 UTC
# 11:30 IST → 06:00 UTC

dag = DAG(
    'bonus_etl',
    default_args=default_args,
    description='ETL DAG to fetch, process, and save employee data with bonus',
    schedule_interval="30,45,0 5,6 * * *",  # Combined UTC times for all 3 runs
    start_date=datetime(2025, 9, 16),
    catchup=False,
)

# Define tasks
fetch_data = BashOperator(
    task_id='fetch_data',
    bash_command='python3 /home/dinesh/airflow_etl/fetch_data.py',
    dag=dag
)

process_data = BashOperator(
    task_id='process_data',
    bash_command='python3 /home/dinesh/airflow_etl/process_data.py',
    dag=dag
)

save_results = BashOperator(
    task_id='save_results',
    bash_command='python3 /home/dinesh/airflow_etl/save_results.py',
    dag=dag
)

# Set task dependencies
fetch_data >> process_data >> save_results
