from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from simple_salesforce import Salesforce
import tempfile
import os
import psycopg2


PIPELINE_NAME = "kdlaw_matter_bulk"

# =========================
# SALESFORCE CONNECT
# =========================
def get_sf():
    conn = BaseHook.get_connection("my_salesforce")
    extra = conn.extra_dejson

    return Salesforce(
        username=extra["username"],
        password=extra["password"],
        security_token=extra["security_token"]
    )


# =========================
# SSH + POSTGRES CONNECT
# =========================
def get_pg_conn_via_ssh():
    ssh = SSHHook(ssh_conn_id="my_ssh")

    # test SSH
    ssh.get_conn()
    print("✅ SSH connected")

    tunnel = ssh.get_tunnel(
        remote_host="127.0.0.1",
        remote_port=5432
    )

    tunnel.start()
    print(f"✅ Tunnel opened at {tunnel.local_bind_port}")

    pg = PostgresHook(postgres_conn_id="my_postgres")
    conn_info = pg.get_connection(pg.postgres_conn_id)

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )

    return conn, tunnel


# =========================
# TASK: BULK + COPY (SSH)
# =========================
import csv

def sync_window(start, end):
    sf = get_sf()

    conn, tunnel = get_pg_conn_via_ssh()
    cursor = conn.cursor()

    try:
        print(f"🚀 Window: {start} → {end}")

        # =========================
        # BULK QUERY (FIXED)
        # =========================
        soql = f"""
        SELECT Id, Name, LastModifiedDate
        FROM kdlaw__Matter__c
        WHERE LastModifiedDate >= {start.strftime('%Y-%m-%dT%H:%M:%SZ')}
          AND LastModifiedDate < {end.strftime('%Y-%m-%dT%H:%M:%SZ')}
        """

        results = sf.bulk2.kdlaw__Matter__c.query(soql)

        # =========================
        # WRITE TEMP CSV (FIXED)
        # =========================
        tmp = tempfile.NamedTemporaryFile(delete=False, mode="w", newline="", suffix=".csv")
        writer = None
        total = 0

        for row in results:
            # ⚠️ guard: skip nếu không phải dict
            if not isinstance(row, dict):
                continue

            if writer is None:
                writer = csv.DictWriter(tmp, fieldnames=row.keys())
                writer.writeheader()

            writer.writerow(row)
            total += 1

        tmp.close()

        print(f"📄 Rows written: {total}")

        # COPY
        with open(tmp.name, "r") as f:
            cursor.copy_expert(
                """
                COPY kdlaw_matter_staging (id, name, lastmodifieddate)
                FROM STDIN WITH CSV HEADER
                """,
                f
            )

        conn.commit()
        os.remove(tmp.name)

        print("✅ Window done")

    finally:
        cursor.close()
        conn.close()
        tunnel.stop()
        print("🔒 Tunnel closed")

# =========================
# MERGE (SSH)
# =========================
def merge_to_final():
    conn, tunnel = get_pg_conn_via_ssh()
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO kdlaw_matter_final
        SELECT * FROM kdlaw_matter_staging
        ON CONFLICT (id)
        DO UPDATE SET
            name = EXCLUDED.name,
            lastmodifieddate = EXCLUDED.lastmodifieddate
        """)

        conn.commit()
        print("✅ Merge done")

    finally:
        cursor.close()
        conn.close()
        tunnel.stop()


# =========================
# WATERMARK (SSH)
# =========================
def update_watermark():
    conn, tunnel = get_pg_conn_via_ssh()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
        INSERT INTO etl_watermark (pipeline_name, last_run)
        VALUES ('{PIPELINE_NAME}', NOW())
        ON CONFLICT (pipeline_name)
        DO UPDATE SET last_run = EXCLUDED.last_run
        """)

        conn.commit()
        print("✅ Watermark updated")

    finally:
        cursor.close()
        conn.close()
        tunnel.stop()


# =========================
# DAG
# =========================
with DAG(
    dag_id="salesforce_bulk_kdlaw_ssh",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["salesforce", "ssh", "bulk"]
) as dag:

    windows = []
    start = datetime(2026, 1, 1)
    end = datetime.utcnow()
    step = timedelta(days=1)

    while start < end:
        next_time = start + step

        t = PythonOperator(
            task_id=f"sync_{start.strftime('%Y%m%d')}",
            python_callable=sync_window,
            op_args=[start, next_time]
        )

        windows.append(t)
        start = next_time

    merge = PythonOperator(
        task_id="merge",
        python_callable=merge_to_final
    )

    wm = PythonOperator(
        task_id="watermark",
        python_callable=update_watermark
    )

    windows >> merge >> wm