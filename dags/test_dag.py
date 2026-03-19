from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_func():
    print("Airflow is working!")

with DAG(
    dag_id="test_dag_demo_github",
    start_date=datetime(2026, 3, 20),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="test_task_demo_github",
        python_callable=test_func
    )