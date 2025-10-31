import os
import json
from dataclasses import asdict
import requests

from typing import List, Optional

from include.urls import YT_CHANNELS_URL, YT_PLAYLIST_ITEMS_URL, YT_VIDEOS_URL
from include.dtos import (
    ChannelDetailsResponse,
    PlaylistItemsPageResponse,
    VideosResponse,
    ExtractedVideo,
    ExtractedVideosResponse,
)

from airflow.decorators import task
from airflow.models import Variable


# Get API credentials from Airflow Variables
def get_api_key() -> str:
    """Get API key from Airflow Variable or return empty string."""
    try:
        return Variable.get("API_KEY", default_var="")
    except:
        return ""


def get_channel_handle() -> str:
    """Get channel handle from Airflow Variable or return default."""
    try:
        return Variable.get("CHANNEL_HANDLE", default_var="MrBeast")
    except:
        return "MrBeast"


@task
def get_channel_details(
    api_key: Optional[str] = None, channel_handle: Optional[str] = None
) -> ChannelDetailsResponse:
    """Fetch channel details from YouTube API."""
    api_key = api_key or get_api_key()
    channel_handle = channel_handle or get_channel_handle()

    url = f"{YT_CHANNELS_URL}?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    response = requests.get(url, timeout=30)
    if response.status_code == 200:
        return ChannelDetailsResponse.from_dict(response.json())
    else:
        raise RuntimeError(
            f"Error fetching channel details: {response.status_code} - {response.text}"
        )


@task
def extract_playlist_id(channel_details: ChannelDetailsResponse) -> str:
    """Extract playlist ID from channel details."""
    items = channel_details.items
    if items:
        return items[0].contentDetails.relatedPlaylists.uploads or ""
    else:
        raise ValueError("No items found in the response")


def _get_videos_page_from_playlist(
    api_key: str,
    playlist_id: str,
    page_token: Optional[str] = None,
    max_results: int = 50,
) -> PlaylistItemsPageResponse:
    """Helper function to fetch a single page of videos from a playlist."""
    url = f"{YT_PLAYLIST_ITEMS_URL}?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={api_key}"
    if page_token:
        url += f"&pageToken={page_token}"

    response = requests.get(url, timeout=30)
    if response.status_code == 200:
        return PlaylistItemsPageResponse.from_dict(response.json())
    else:
        raise RuntimeError(
            f"Error fetching videos from playlist: {response.status_code} - {response.text}"
        )


@task
def get_videos_from_playlist(
    playlist_id: str, api_key: Optional[str] = None
) -> List[str]:
    """Fetch all video IDs from a playlist with pagination."""
    api_key = api_key or get_api_key()

    video_ids: List[str] = []
    page_token = None
    while True:
        response = _get_videos_page_from_playlist(api_key, playlist_id, page_token)
        for item in response.items:
            vid = item.contentDetails.videoId
            if vid:
                video_ids.append(vid)
        page_token = response.nextPageToken
        if not page_token:
            break
    return video_ids


@task
def get_video_details(
    video_ids: List[str], api_key: Optional[str] = None, batch_size: int = 50
) -> ExtractedVideosResponse:
    """Fetch video details in batches."""
    api_key = api_key or get_api_key()

    extracted: List[ExtractedVideo] = []

    # Process videos in batches
    for batch_start in range(0, len(video_ids), batch_size):
        video_batch = video_ids[batch_start : batch_start + batch_size]
        ids = ",".join(video_batch)
        if not ids:
            continue

        url = f"{YT_VIDEOS_URL}?part=contentDetails&part=snippet&part=statistics&id={ids}&key={api_key}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = VideosResponse.from_dict(response.json())
            if not data.items:
                raise ValueError("No items found in the response")

            for item in data.items:
                if not item.id:
                    raise ValueError("Video ID not found in the response")
                extracted.append(ExtractedVideo.from_video_item(item))
        else:
            raise RuntimeError(
                f"Error fetching video details: {response.status_code} - {response.text}"
            )
    return ExtractedVideosResponse(items=extracted)


def main() -> None:
    """Main function for standalone execution (non-Airflow)."""
    api_key = os.getenv("API_KEY", "")
    channel_handle = os.getenv("CHANNEL_HANDLE", "MrBeast")

    # Get channel details
    url = f"{YT_CHANNELS_URL}?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Error fetching channel details: {response.status_code} - {response.text}"
        )

    channel_details = ChannelDetailsResponse.from_dict(response.json())

    # Extract playlist ID
    items = channel_details.items
    if not items:
        raise ValueError("No items found in the response")
    uploads_playlist_id = items[0].contentDetails.relatedPlaylists.uploads or ""
    if not uploads_playlist_id:
        raise ValueError("Playlist ID not found in the response")

    # Get video IDs
    video_ids: List[str] = []
    page_token = None
    while True:
        response = _get_videos_page_from_playlist(
            api_key, uploads_playlist_id, page_token
        )
        for item in response.items:
            vid = item.contentDetails.videoId
            if vid:
                video_ids.append(vid)
        page_token = response.nextPageToken
        if not page_token:
            break

    # Get video details
    extracted: List[ExtractedVideo] = []
    batch_size = 50
    for batch_start in range(0, len(video_ids), batch_size):
        video_batch = video_ids[batch_start : batch_start + batch_size]
        ids = ",".join(video_batch)
        if not ids:
            continue

        url = f"{YT_VIDEOS_URL}?part=contentDetails&part=snippet&part=statistics&id={ids}&key={api_key}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = VideosResponse.from_dict(response.json())
            if not data.items:
                raise ValueError("No items found in the response")

            for item in data.items:
                if not item.id:
                    raise ValueError("Video ID not found in the response")
                extracted.append(ExtractedVideo.from_video_item(item))
        else:
            raise RuntimeError(
                f"Error fetching video details: {response.status_code} - {response.text}"
            )

    details = ExtractedVideosResponse(items=extracted)

    os.makedirs("./data", exist_ok=True)
    serializable = [asdict(v) for v in details.items]
    with open("./data/video_stats.json", "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=4)
    print("Video stats saved to ./data/video_stats.json")


if __name__ == "__main__":
    main()
