import httpx
from typing import Optional

from src.config import settings

# Tags that appear frequently on Last.fm but carry no analytical signal.
# These are personal markers, UI artifacts, or generic descriptors that would
# pollute genre/mood analysis if kept.
# TODO: put this in a config file or something for cleaner code files.
NOISE_TAGS = {
    "seen live",
    "favourite",
    "favorites",
    "love",
    "loved",
    "awesome",
    "good",
    "bad",
    "cool",
    "my favorite",
    "favorite",
    "amazing",
    "great",
    "best",
    "beautiful",
    "nice",
    "songs i like",
    "my music",
    "check",
    "todo",
    "to listen",
    "heard on rdio",
    "spotify",
    "youtube",
    "under 2000 listeners",
    "all",
    "music",
}


class LastfmClient:
    """
    Handles all communication with the Last.fm REST API.
    """

    BASE_URL = "https://ws.audioscrobbler.com/2.0/"

    def __init__(self):
        self._api_key = settings.LASTFM_API_KEY
        self._http_client = httpx.Client(timeout=10.0)

    def get_track_info(self, track_name: str, artist_name: str) -> Optional[dict]:
        """
        Fetches metadata for a single track from Last.fm's track.getInfo endpoint.

        Returns a dict with:
            tags      — list of up to 5 filtered tag names, sorted by relevance
            top_tag   — the first tage (most relevant), or None if no tags found
            listeners — global unique listener count
            playcount — global total play count

        Returns None if the track is not found or if the request fails.
        """
        try:
            # autocorrect=1 lets Last.fm fix minor spelling differences in track/artist names
            response = self._http_client.get(
                self.BASE_URL,
                params={
                    "method": "track.getInfo",
                    "track": track_name,
                    "artist": artist_name,
                    "api_key": self._api_key,
                    "format": "json",
                    "autocorrect": 1,
                },
            )
            response.raise_for_status()
            data = response.json()

            # Last.fm returns {"error": <code>, "message": "..."} for not-found tracks
            # instead of using HTTP error codes.
            if "error" in data:
                return None

            track = data.get("track", {})

            # toptags.tag is a list of {name, url} dicts, but Last.fm returns a dict
            # instead of a list when there is only one tag. Normalise to always be a list.
            raw_tags = track.get("toptags", {}).get("tag", [])
            if isinstance(raw_tags, dict):
                raw_tags = [raw_tags]

            filtered_tags = [
                t["name"].lower()
                for t in raw_tags
                if t.get("name", "").lower() not in NOISE_TAGS
            ][:5]

            # listeners and playcount come back as strings from the API
            return {
                "tags": filtered_tags,
                "top_tag": filtered_tags[0] if filtered_tags else None,
                "listeners": int(track.get("listeners") or 0),
                "playcount": int(track.get("playcount") or 0),
            }

        except Exception:
            return None

    def get_artist_tags(self, artist_name: str) -> Optional[dict]:
        """
        Fetches top tags for an artist from Last.fm's artist.getTopTags endpoint.

        Artist-level tags are better populated than track-level tags:
        most artists have at least a few genre/mood tags even when individual tracks
        don't. We put artist tags the primary source of genre signal in the pipeline.

        Returns a dict with:
            tags    — list of up to 5 filtered tag names
            top_tag — the first tag, or None

        Returns None if the artist is not found or the request fails.
        """
        try:
            response = self._http_client.get(
                self.BASE_URL,
                params={
                    "method": "artist.getTopTags",
                    "artist": artist_name,
                    "api_key": self._api_key,
                    "format": "json",
                    "autocorrect": 1,
                },
            )
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                return None

            # Same single-item normalisation as track tags
            raw_tags = data.get("toptags", {}).get("tag", [])
            if isinstance(raw_tags, dict):
                raw_tags = [raw_tags]

            filtered_tags = [
                t["name"].lower()
                for t in raw_tags
                if t.get("name", "").lower() not in NOISE_TAGS
            ][:5]

            return {
                "tags": filtered_tags,
                "top_tag": filtered_tags[0] if filtered_tags else None,
            }

        except Exception:
            return None
