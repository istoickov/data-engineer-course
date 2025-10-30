import os
import json
import requests

from dotenv import load_dotenv

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = "MrBeast"
URL = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle=MrBeast&key={API_KEY}"


if __name__ == "__main__":

    response = requests.get(URL)
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        raise Exception("Failed to fetch data from YouTube API")

    items = data.get("items", [])
    if items:
        playlist_id = (
            items[0]
            .get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads", "")
        )
    else:
        raise ValueError("No items found in the response")

    if not playlist_id:
        raise ValueError("Playlist ID not found in the response")

    print(f"Playlist ID: {playlist_id}")
