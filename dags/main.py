import os
import sys
import pendulum

from pathlib import Path

from airflow import DAG

from datetime import datetime, timedelta
from api.video_stats import (
    get_channel_details,
    extract_playlist_id,
    get_videos_from_playlist,
    get_video_details,
)
from datawarehouse.dwh import staging_table, core_table

# Add dags directory to Python path for imports
dags_folder = Path(__file__).parent
if str(dags_folder) not in sys.path:
    sys.path.insert(0, str(dags_folder))


local_tz = pendulum.timezone("Europe/Skopje")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "email": "iv.stoickov@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2023, 10, 1, tzinfo=local_tz),
}

with DAG(
    dag_id="youtube_video_stats",
    default_args=default_args,
    description="Fetch YouTube video statistics",
    schedule="0 14 * * *",
    catchup=False,
    tags=["youtube", "video_stats"],
) as dag:
    # Fetch channel details
    channel_details_task = get_channel_details()

    # Get playlist ID from channel details
    playlist_id_task = extract_playlist_id(channel_details_task)

    # Get video IDs from the playlist
    video_ids_task = get_videos_from_playlist(playlist_id_task)

    # Fetch video details in batches
    # Task dependencies are automatically handled by TaskFlow API
    video_details_task = get_video_details(video_ids_task)

with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="Update the database with YouTube video statistics",
    schedule="0 15 * * *",
    catchup=False,
    tags=["youtube", "video_stats"],
) as dag:
    # Create staging table and insert data
    staging_table_task = staging_table()

    # Create the core table and insert data
    core_table_task = core_table()
