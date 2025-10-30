import os
import json
from dataclasses import asdict
import requests

from typing import List, Optional
from dotenv import load_dotenv

from urls import YT_CHANNELS_URL, YT_PLAYLIST_ITEMS_URL, YT_VIDEOS_URL
from dtos import (
    ChannelDetailsResponse,
    PlaylistItemsPageResponse,
    VideosResponse,
    ExtractedVideo,
    ExtractedVideosResponse,
)

load_dotenv(dotenv_path="./.env")

API_KEY = os.getenv("API_KEY") or ""
CHANNEL_HANDLE = "MrBeast"


class YouTubeWrapper:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_channel_details(self, channel_handle: str) -> ChannelDetailsResponse:
        url = f"{YT_CHANNELS_URL}?part=contentDetails&forHandle={channel_handle}&key={self.api_key}"
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return ChannelDetailsResponse.from_dict(response.json())
        else:
            raise RuntimeError(
                f"Error fetching channel details: {response.status_code} - {response.text}"
            )

    def get_playlist_id(self, channel_handle: str) -> str:
        channel_details = self.get_channel_details(channel_handle)
        items = channel_details.items
        if items:
            return items[0].contentDetails.relatedPlaylists.uploads or ""
        else:
            raise ValueError("No items found in the response")

    def get_videos_page_from_playlist(
        self, playlist_id: str, page_token: Optional[str] = None, max_results: int = 50
    ) -> PlaylistItemsPageResponse:
        url = f"{YT_PLAYLIST_ITEMS_URL}?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={self.api_key}"
        if page_token:
            url += f"&pageToken={page_token}"

        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return PlaylistItemsPageResponse.from_dict(response.json())
        else:
            raise RuntimeError(
                f"Error fetching videos from playlist: {response.status_code} - {response.text}"
            )

    def get_videos_from_playlist(self, playlist_id: str) -> List[str]:
        video_ids: List[str] = []
        page_token = None
        while True:
            response = self.get_videos_page_from_playlist(playlist_id, page_token)
            for item in response.items:
                vid = item.contentDetails.videoId
                if vid:
                    video_ids.append(vid)
            page_token = response.nextPageToken
            if not page_token:
                break
        return video_ids

    def get_video_batches(self, videos: List[str], batch_size: int = 50):
        for batch_start in range(0, len(videos), batch_size):
            yield videos[batch_start : batch_start + batch_size]

    def get_video_details(self, video_ids: List[str]) -> ExtractedVideosResponse:
        extracted: List[ExtractedVideo] = []
        for video_batch in self.get_video_batches(video_ids):
            ids = ",".join(video_batch)
            if not ids:
                continue

            url = f"{YT_VIDEOS_URL}?part=contentDetails&part=snippet&part=statistics&id={ids}&key={self.api_key}"
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
    yt_wrapper = YouTubeWrapper(API_KEY)
    uploads_playlist_id = yt_wrapper.get_playlist_id(CHANNEL_HANDLE)
    if not uploads_playlist_id:
        raise ValueError("Playlist ID not found in the response")

    ids = yt_wrapper.get_videos_from_playlist(uploads_playlist_id)
    details = yt_wrapper.get_video_details(ids)

    os.makedirs("./data", exist_ok=True)
    serializable = [asdict(v) for v in details.items]
    with open("./data/video_stats.json", "w", encoding="utf-8") as f:
        json.dump(serializable, f, ensure_ascii=False, indent=4)
    print("Video stats saved to ./data/video_stats.json")


if __name__ == "__main__":
    main()
