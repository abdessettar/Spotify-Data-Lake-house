from datetime import datetime
from typing import List, Optional, Dict
from pydantic import BaseModel, Field, AliasChoices


class PipelineCursor(BaseModel):
    """
    Tracks the state of the pipeline.
    Stored in 'system/state.json' in the Data Lake.
    """

    last_run_timestamp: datetime
    # We use 'after' (unix timestamp in ms) for Spotify API pagination
    last_played_at_unix_ms: int


class SpotifyArtist(BaseModel):
    id: str
    name: str
    type: str
    uri: str
    href: str
    external_urls: Dict[str, str]


class SpotifyImage(BaseModel):
    url: str
    height: Optional[int] = None
    width: Optional[int] = None


class SpotifyAlbum(BaseModel):
    id: str
    name: str
    album_type: str
    total_tracks: int
    release_date: str
    release_date_precision: str
    type: str
    uri: str
    href: str
    images: List[SpotifyImage]
    artists: List[SpotifyArtist]


class SpotifyTrack(BaseModel):
    id: str
    name: str
    duration_ms: int
    explicit: bool
    popularity: Optional[int] = None
    track_number: int
    type: str
    uri: str
    href: str
    album: SpotifyAlbum
    artists: List[SpotifyArtist]
    is_local: bool
    external_ids: Optional[Dict[str, str]] = None


class LastfmTrackInfo(BaseModel):
    """
    Validated record stored in Bronze after a Last.fm track.getInfo call.
    One file per track in bronze/lastfm_enrichment/<base64(track_id)>.json.
    """

    track_id: str
    tags: List[str]
    top_tag: Optional[str] = None
    listeners: int = 0
    playcount: int = 0


class SpotifyContext(BaseModel):
    type: str
    uri: str
    href: str
    external_urls: Dict[str, str]


class PlayedItem(BaseModel):
    """
    Represents one row in the 'Recently Played' response.
    """

    track: SpotifyTrack
    played_at: str
    context: Optional[SpotifyContext] = None


class RecentlyPlayedResponse(BaseModel):
    """
    The full response from Spotify /v1/me/player/recently-played
    """

    items: List[PlayedItem]
    next: Optional[str] = None
    cursors: Optional[dict] = None


class SpotifyExportRecord(BaseModel):
    """
    Schema for Spotify's standard Account Data Export (StreamingHistory.json)
    or Extended History (endsong.json).
    """

    # AliasChoices maps "endTime" (standard export) OR "ts" (extended export) → played_at
    played_at: str = Field(validation_alias=AliasChoices("endTime", "ts"))

    # Standard export uses "artistName"; extended uses "master_metadata_album_artist_name"
    artist_name: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices(
            "artistName", "master_metadata_album_artist_name"
        ),
    )

    # Standard export uses "trackName"; extended uses "master_metadata_track_name"
    track_name: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("trackName", "master_metadata_track_name"),
    )

    duration_ms: int = Field(validation_alias=AliasChoices("msPlayed", "ms_played"))

    # spotify_track_uri is only present in the extended export format (endsong_*.json);
    # standard exports don't include a Spotify URI, so this defaults to None
    spotify_track_uri: Optional[str] = Field(
        default=None, validation_alias=AliasChoices("spotify_track_uri", "uri")
    )

    # Extended history fields (endsong_*.json only, None for standard exports)
    platform: Optional[str] = None
    conn_country: Optional[str] = None
    master_metadata_album_album_name: Optional[str] = None
    reason_start: Optional[str] = None
    reason_end: Optional[str] = None
    shuffle: Optional[bool] = None
    skipped: Optional[bool] = None
    offline: Optional[bool] = None
    incognito_mode: Optional[bool] = None

    @property
    def extracted_track_id(self) -> Optional[str]:
        """Extract track ID from spotify_track_uri like 'spotify:track:XXXXX'."""
        # Spotify URIs follow the format "spotify:track:<id>", so we split on ":"
        # and take the last segment to get the raw track ID for API lookups
        if self.spotify_track_uri and self.spotify_track_uri.startswith(
            "spotify:track:"
        ):
            return self.spotify_track_uri.split(":")[-1]
        return None
