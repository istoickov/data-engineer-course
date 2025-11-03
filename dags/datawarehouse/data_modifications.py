import logging

from data_engineer_course.dags.datawarehouse.data_loading import load_data
from data_engineer_course.dags.datawarehouse.utils import (
    get_conn_cursor,
    close_conn_cursor,
)

logger = logging.getLogger(__name__)


def insert_rows(schema, table="video_stats", data=None):
    """Insert rows into the data warehouse."""

    conn, cursor = get_conn_cursor()

    try:
        if schema == "staging":
            insert_sql = f"""
                INSERT INTO {schema}.{table} (video_id, video_title, upload_date, duration, video_views, video_likes, video_comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING;
            """
        else:
            insert_sql = f"""
                INSERT INTO {schema}.{table} (video_id, video_title, upload_date, duration, video_type, video_views, video_likes, video_comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING;
            """

        for row in data:
            cursor.execute(
                insert_sql,
                (
                    row["video_id"],
                    row["video_title"],
                    row["upload_date"],
                    row["duration"],
                    row.get("video_type", None),  # Optional for staging schema
                    row["video_views"],
                    row["video_likes"],
                    row["video_comments"],
                ),
            )
        conn.commit()
        logger.info("Rows inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting rows: {e}")
        conn.rollback()
    finally:
        close_conn_cursor(conn, cursor)


def update_rows(schema, table="video_stats", data=None):
    """Update rows in the data warehouse."""

    conn, cursor = get_conn_cursor()

    try:
        if schema == "staging":
            update_sql = f"""
                UPDATE {schema}.{table}
                SET video_title = %s, upload_date = %s, duration = %s, video_views = %s, video_likes = %s, video_comments = %s
                WHERE video_id = %s;
            """
        else:
            update_sql = f"""
                UPDATE {schema}.{table}
                SET video_title = %s, upload_date = %s, duration = %s, video_type = %s, video_views = %s, video_likes = %s, video_comments = %s
                WHERE video_id = %s;
            """

        for row in data:
            cursor.execute(
                update_sql,
                (
                    row["video_title"],
                    row["upload_date"],
                    row["duration"],
                    row.get("video_type", None),  # Optional for staging schema
                    row["video_views"],
                    row["video_likes"],
                    row["video_comments"],
                    row["video_id"],
                ),
            )
        conn.commit()
        logger.info("Rows updated successfully.")
    except Exception as e:
        logger.error(f"Error updating rows: {e}")
        conn.rollback()
    finally:
        close_conn_cursor(conn, cursor)


def delete_rows(schema, table="video_stats", video_ids=None):
    """Delete rows from the data warehouse."""

    if not video_ids:
        logger.warning("No video IDs provided for deletion.")
        return

    conn, cursor = get_conn_cursor()

    try:
        delete_sql = f"DELETE FROM {schema}.{table} WHERE video_id = ANY(%s);"
        cursor.execute(delete_sql, (video_ids,))
        conn.commit()
        logger.info(f"Deleted {cursor.rowcount} rows successfully.")
    except Exception as e:
        logger.error(f"Error deleting rows: {e}")
        conn.rollback()
    finally:
        close_conn_cursor(conn, cursor)

