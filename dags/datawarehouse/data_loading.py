import json
import logging

from datetime import date


logger = logging.getLogger(__name__)


def load_data():
    """Load data from a JSON file."""

    file_path = f"./data/video_stats.json"

    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        logging.info(f"Data loaded successfully: {data}")
        return data
    except FileNotFoundError:
        logging.error("Data file not found.")
        raise
    except json.JSONDecodeError:
        logging.error("Error decoding JSON data.")
        raise
