from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =========================
# TASK: QUERY DATA VIA SSH TUNNEL
# =========================
def query_chunks():
    ssh = SSHHook(ssh_conn_id="my_ssh")

    # tạo SSH tunnel
    with ssh.get_tunnel(
        remote_host="DB_HOST",   # ⚠️ thay bằng host DB thật
        remote_port=5432
    ) as tunnel:

        pg = PostgresHook(
            postgres_conn_id="my_postgres",
            port=tunnel.local_bind_port
        )

        rows = pg.get_records("""
            SELECT *
            FROM chunks
            LIMIT 100
        """)

        print(f"Fetched {len(rows)} rows")

        # log 5 dòng đầu
        for r in rows[:5]:
            print(r)


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="chunks_via_ssh_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # chạy manual
    catchup=False,
    tags=["ssh", "postgres", "etl"]
) as dag:

    t1 = PythonOperator(
        task_id="query_chunks",
        python_callable=query_chunks,
    )

    t1