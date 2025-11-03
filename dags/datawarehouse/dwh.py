import logging

from airflow.decorators import task

from dags.datawarehouse.data_modifications import insert_rows, update_rows, delete_rows
from dags.datawarehouse.data_transformations import transform_video_data
from dags.datawarehouse.data_loading import load_data
from dags.datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids,
)


logger = logging.getLogger(__name__)

table = "video_stats"


@task
def staging_table():
    """Create staging table and insert data."""
    schema = "staging"
    conn, cursor = get_conn_cursor()

    try:
        yt_data = load_data()
        create_schema(schema)
        create_table(schema, table)
        table_ids = get_video_ids(schema, table)
        insert_rows_data, update_rows_data = [], []
        for row in yt_data:
            if len(table_ids) == 0:
                insert_rows(schema, table, yt_data)
                logger.info(
                    f"Data inserted into staging table {schema}.{table} successfully."
                )
            else:
                if row["video_id"] not in table_ids:
                    insert_rows_data.append(row)
                else:
                    update_rows_data.append(row)

        if insert_rows_data:
            insert_rows(schema, table, insert_rows_data)
            logger.info(f"Inserted new rows into staging table {schema}.{table}.")
        if update_rows_data:
            update_rows(schema, table, update_rows_data)
            logger.info(f"Updated existing rows in staging table {schema}.{table}.")

        ids_in_json = set(row["video_id"] for row in yt_data)
        ids_to_delete = set(table_ids) - ids_in_json
        if ids_to_delete:
            delete_rows(schema, table, ids_to_delete)
            logger.info(f"Deleted rows from staging table {schema}.{table}.")

        logger.info(f"Staging table {schema}.{table} processed successfully.")

    except Exception as e:
        logger.error(f"Error processing staging table: {e}")
        conn.rollback()
    finally:
        close_conn_cursor(conn, cursor)


@task
def core_table():
    """Create core table and transform data."""
    schema = "core"
    conn, cursor = get_conn_cursor()

    try:
        create_schema(schema)
        create_table(schema, table)
        staging_data = load_data()
        table_ids = get_video_ids(schema, table)

        cursor.execute(f"SELECT * FROM {schema}.{table};")
        existing_rows = cursor.fetchall()

        insert_rows_data, update_rows_data = [], []

        current_video_ids = set()
        for row in staging_data:
            current_video_ids.add(row["video_id"])

            if len(table_ids) == 0:
                transform_data = transform_video_data(row)
                insert_rows_data.append(transform_data)
            else:
                if row["video_id"] not in table_ids:
                    transform_data = transform_video_data(row)
                    insert_rows_data.append(transform_data)
                else:
                    update_rows_data.append(row)

        if insert_rows_data:
            insert_rows(schema, table, insert_rows_data)
            logger.info(f"Inserted new rows into core table {schema}.{table}.")
        if update_rows_data:
            update_rows(schema, table, update_rows_data)
            logger.info(f"Updated existing rows in core table {schema}.{table}.")

        ids_to_delete = set(table_ids) - current_video_ids
        if ids_to_delete:
            delete_rows(schema, table, ids_to_delete)
            logger.info(f"Deleted rows from core table {schema}.{table}.")
        logger.info(f"Core table {schema}.{table} processed successfully.")
    except Exception as e:
        logger.error(f"Error processing core table: {e}")
        conn.rollback()
    finally:
        close_conn_cursor(conn, cursor)
