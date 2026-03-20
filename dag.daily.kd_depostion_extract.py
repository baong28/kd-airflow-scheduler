from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from sshtunnel import SSHTunnelForwarder
import tempfile
import os

# =========================
# ENV (thay vì dùng streamlit secrets)
# =========================
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_PRIVATE_KEY = os.getenv("SSH_PRIVATE_KEY")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")


if not SSH_PRIVATE_KEY:
    raise ValueError("SSH_PRIVATE_KEY is missing!")

# =========================
# HELPER: SSH + DB CONNECT
# =========================
def get_connection():
    key_file = tempfile.NamedTemporaryFile(delete=False, mode="w")
    key_file.write(SSH_PRIVATE_KEY.strip())
    key_file.close()

    ssh_key_path = key_file.name

    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=ssh_key_path,
        allow_agent=False,
        host_pkey_directories=[],
        remote_bind_address=(DB_HOST, DB_PORT),
    )

    tunnel.start()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=tunnel.local_bind_port,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    return conn, tunnel

# =========================
# TASK 1: GET FILENAMES
# =========================
def get_filenames(**context):
    conn, tunnel = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT filename
                FROM chunks
                ORDER BY filename ASC
            """)
            rows = cur.fetchall()

        filenames = [r[0] for r in rows]

        # push XCom
        context["ti"].xcom_push(key="filenames", value=filenames)

    finally:
        conn.close()
        tunnel.stop()


# =========================
# TASK 2: GET STATS
# =========================
def get_stats(**context):
    conn, tunnel = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    filename,
                    COUNT(DISTINCT page),
                    COUNT(*)
                FROM chunks
                WHERE issue_extracted = 1
                GROUP BY filename
            """)

            rows = cur.fetchall()

        stats = {
            r[0]: {"pages": r[1], "chunks": r[2]}
            for r in rows
        }

        context["ti"].xcom_push(key="stats", value=stats)

    finally:
        conn.close()
        tunnel.stop()


# =========================
# TASK 3: GET ISSUES
# =========================
def get_issues(**context):
    filenames = context["ti"].xcom_pull(key="filenames")

    if not filenames:
        return []

    conn, tunnel = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    issue_id,
                    chunk_id,
                    filename,
                    page,
                    speaker_role,
                    risk_level,
                    legal_relevance,
                    quoted_text,
                    issue_type,
                    pdf_link
                FROM deposition_issues
                WHERE filename = ANY(%s)
                ORDER BY filename, page
            """, (filenames,))

            rows = cur.fetchall()

        # ví dụ: log số lượng
        print(f"Total issues: {len(rows)}")

    finally:
        conn.close()
        tunnel.stop()


# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="deposition_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "ssh", "postgres"]
) as dag:

    t1 = PythonOperator(
        task_id="get_filenames",
        python_callable=get_filenames,
    )

    t2 = PythonOperator(
        task_id="get_stats",
        python_callable=get_stats,
    )

    t3 = PythonOperator(
        task_id="get_issues",
        python_callable=get_issues,
    )

    # flow
    t1 >> t2 >> t3