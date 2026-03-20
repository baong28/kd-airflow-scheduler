from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def query_with_tunnel():
    ssh = SSHHook(ssh_conn_id="my_ssh")

    with ssh.get_tunnel(
        remote_port=5432,
        remote_host="DB_HOST"
    ) as tunnel:

        pg = PostgresHook(
            postgres_conn_id="my_postgres",
            port=tunnel.local_bind_port
        )

        rows = pg.get_records("SELECT * FROM chunks")
        return rows