from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import RealDictCursor


def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor


def close_conn_cursor(conn, cursor):
    """Close the connection and cursor."""
    if cursor:
        cursor.close()
    if conn:
        conn.close()


def create_schema(schema):
    conn, cursor = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cursor.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)


def create_table(schema, table):
    conn, cursor = get_conn_cursor()

    if schema == "staging":
        tabke_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                video_id VARCHAR(255) PRIMARY KEY,
                video_title TEXT NOT NULL,
                upload_date TIMESTAMP NOT NULL,
                duration VARCHAR(20) NOT NULL,
                video_views INTEGER,
                video_likes INTEGER,
                video_comments INTEGER,
            );
        """
    else:
        tabke_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                video_id VARCHAR(255) PRIMARY KEY,
                video_title TEXT NOT NULL,
                upload_date TIMESTAMP NOT NULL,
                duration VARCHAR(20) NOT NULL,
                video_type VARCHAR(50) NOT NULL,
                video_views INTEGER,
                video_likes INTEGER,
                video_comments INTEGER,
            );
        """

    cursor.execute(tabke_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)


def get_video_ids(schema, table):
    conn, cursor = get_conn_cursor()
    query = f"SELECT video_id FROM {schema}.{table};"
    cursor.execute(query)
    video_ids = [row["video_id"] for row in cursor.fetchall()]
    close_conn_cursor(conn, cursor)
    return video_ids

