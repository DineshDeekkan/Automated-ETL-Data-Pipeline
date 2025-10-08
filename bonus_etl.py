from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import timezone   # ✅ required

# Filters for DAG run
job_title = "Data Analyst"
min_salary = 75000

default_args = {
    'owner': 'dinesh',
    'depends_on_past': False,
    'retries': 0,
}

local_tz = timezone("Asia/Kolkata")  # ✅ IST timezone

dag = DAG(
    'bonus_etl',
    default_args=default_args,
    description='ETL DAG to fetch, process, and save employee data with bonus',
    schedule_interval="0,10,20 10,13,16 * * *",
    start_date=datetime(2025, 9, 25, 10, 0, tzinfo=local_tz),
    catchup=False,
)

fetch_data = BashOperator(
    task_id='fetch_data',
    bash_command=f'python3 /home/dinesh/airflow_etl/fetch_data.py --job_title "{job_title}" --min_salary {min_salary}',
    dag=dag
)

process_data = BashOperator(
    task_id='process_data',
    bash_command=f'python3 /home/dinesh/airflow_etl/process_data.py --job_title "{job_title}" --min_salary {min_salary}',
    dag=dag
)

save_results = BashOperator(
    task_id='save_results',
    bash_command=f'python3 /home/dinesh/airflow_etl/save_results.py --job_title "{job_title}" --min_salary {min_salary}',
    dag=dag
)

fetch_data >> process_data >> save_results
