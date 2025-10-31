from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


# Channel
@dataclass(frozen=True)
class RelatedPlaylists:
    uploads: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> RelatedPlaylists:
        return RelatedPlaylists(uploads=data.get("uploads"))


@dataclass(frozen=True)
class ChannelContentDetails:
    relatedPlaylists: RelatedPlaylists

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ChannelContentDetails:
        return ChannelContentDetails(
            relatedPlaylists=RelatedPlaylists.from_dict(
                data.get("relatedPlaylists", {})
            )
        )


@dataclass(frozen=True)
class ChannelItem:
    contentDetails: ChannelContentDetails

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ChannelItem:
        return ChannelItem(
            contentDetails=ChannelContentDetails.from_dict(
                data.get("contentDetails", {})
            )
        )


@dataclass(frozen=True)
class ChannelDetailsResponse:
    items: List[ChannelItem]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> ChannelDetailsResponse:
        items = [ChannelItem.from_dict(item) for item in data.get("items", [])]
        return ChannelDetailsResponse(items=items)


# Playlist items (uploads)
@dataclass(frozen=True)
class PlaylistItemContentDetails:
    videoId: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> PlaylistItemContentDetails:
        return PlaylistItemContentDetails(videoId=data.get("videoId"))


@dataclass(frozen=True)
class PlaylistItem:
    contentDetails: PlaylistItemContentDetails

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> PlaylistItem:
        return PlaylistItem(
            contentDetails=PlaylistItemContentDetails.from_dict(
                data.get("contentDetails", {})
            )
        )


@dataclass(frozen=True)
class PlaylistItemsPageResponse:
    items: List[PlaylistItem]
    nextPageToken: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> PlaylistItemsPageResponse:
        items = [PlaylistItem.from_dict(item) for item in data.get("items", [])]
        return PlaylistItemsPageResponse(
            items=items, nextPageToken=data.get("nextPageToken")
        )


# Videos details
@dataclass(frozen=True)
class VideoSnippet:
    channelId: Optional[str]
    channelTitle: Optional[str]
    publishedAt: Optional[str]
    title: Optional[str]
    description: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> VideoSnippet:
        return VideoSnippet(
            channelId=data.get("channelId"),
            channelTitle=data.get("channelTitle"),
            publishedAt=data.get("publishedAt"),
            title=data.get("title"),
            description=data.get("description"),
        )


@dataclass(frozen=True)
class VideoStatistics:
    viewCount: Optional[str]
    likeCount: Optional[str]
    commentCount: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> VideoStatistics:
        return VideoStatistics(
            viewCount=data.get("viewCount"),
            likeCount=data.get("likeCount"),
            commentCount=data.get("commentCount"),
        )


@dataclass(frozen=True)
class VideoContentDetails:
    duration: Optional[str]
    definition: Optional[str]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> VideoContentDetails:
        return VideoContentDetails(
            duration=data.get("duration"),
            definition=data.get("definition"),
        )


@dataclass(frozen=True)
class VideoItem:
    id: str
    snippet: VideoSnippet
    statistics: VideoStatistics
    contentDetails: VideoContentDetails

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> VideoItem:
        return VideoItem(
            id=data.get("id", ""),
            snippet=VideoSnippet.from_dict(data.get("snippet", {})),
            statistics=VideoStatistics.from_dict(data.get("statistics", {})),
            contentDetails=VideoContentDetails.from_dict(
                data.get("contentDetails", {})
            ),
        )


@dataclass(frozen=True)
class VideosResponse:
    items: List[VideoItem]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> VideosResponse:
        items = [VideoItem.from_dict(item) for item in data.get("items", [])]
        return VideosResponse(items=items)


# Convenience shapes matching your current extraction
@dataclass(frozen=True)
class ExtractedSnippet:
    channel_id: Optional[str]
    channel_title: Optional[str]
    published_at: Optional[str]
    title: Optional[str]
    description: Optional[str]


@dataclass(frozen=True)
class ExtractedStatistics:
    view_count: Optional[int]
    like_count: Optional[int]
    comment_count: Optional[int]


@dataclass(frozen=True)
class ExtractedContentDetails:
    duration: Optional[str]
    definition: Optional[str]


@dataclass(frozen=True)
class ExtractedVideo:
    video_id: str
    snippet: ExtractedSnippet
    statistics: ExtractedStatistics
    content_details: ExtractedContentDetails

    @staticmethod
    def from_video_item(item: VideoItem) -> ExtractedVideo:
        def to_int(value: Optional[str]) -> Optional[int]:
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        return ExtractedVideo(
            video_id=item.id,
            snippet=ExtractedSnippet(
                channel_id=item.snippet.channelId,
                channel_title=item.snippet.channelTitle,
                published_at=item.snippet.publishedAt,
                title=item.snippet.title,
                description=item.snippet.description,
            ),
            statistics=ExtractedStatistics(
                view_count=to_int(item.statistics.viewCount),
                like_count=to_int(item.statistics.likeCount),
                comment_count=to_int(item.statistics.commentCount),
            ),
            content_details=ExtractedContentDetails(
                duration=item.contentDetails.duration,
                definition=item.contentDetails.definition,
            ),
        )


@dataclass(frozen=True)
class ExtractedVideosResponse:
    items: List[ExtractedVideo]

    @staticmethod
    def from_videos_response(resp: VideosResponse) -> ExtractedVideosResponse:
        return ExtractedVideosResponse(
            items=[ExtractedVideo.from_video_item(item) for item in resp.items]
        )
