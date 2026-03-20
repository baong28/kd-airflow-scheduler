from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_chunks():
    # =========================
    # 1. SSH CONNECT
    # =========================
    ssh = SSHHook(ssh_conn_id="my_ssh")

    # debug SSH (nếu lỗi sẽ fail tại đây)
    ssh_client = ssh.get_conn()
    print("✅ SSH connected successfully")

    # =========================
    # 2. CREATE TUNNEL
    # =========================
    with ssh.get_tunnel(
        remote_host="127.0.0.1",   # DB chạy cùng server SSH
        remote_port=5432
    ) as tunnel:

        print(f"✅ Tunnel opened at port: {tunnel.local_bind_port}")

        # =========================
        # 3. GET DB CREDENTIAL
        # =========================
        pg = PostgresHook(postgres_conn_id="my_postgres")
        conn_info = pg.get_connection(pg.postgres_conn_id)

        # =========================
        # 4. CONNECT DB QUA TUNNEL
        # =========================
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )

        # =========================
        # 5. QUERY
        # =========================
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM chunks
                LIMIT 100
            """)
            rows = cur.fetchall()

        conn.close()

        print(f"✅ Fetched {len(rows)} rows")


# =========================
# DAG
# =========================
with DAG(
    dag_id="chunks_via_ssh_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ssh", "postgres"]
) as dag:

    t1 = PythonOperator(
        task_id="query_chunks",
        python_callable=query_chunks,
        retries=2
    )

    t1