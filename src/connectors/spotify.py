import base64
import httpx
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.domain.schemas import RecentlyPlayedResponse


class SpotifyClient:
    """
    Handles communication with the Spotify Web API.
    - Manages token refresh automatically.
    - Retries on transient errors.
    """

    def __init__(self):
        self._client_id = settings.SPOTIFY_CLIENT_ID
        self._client_secret = settings.SPOTIFY_CLIENT_SECRET
        self._refresh_token = settings.SPOTIFY_REFRESH_TOKEN
        self._access_token: Optional[str] = None
        self._http_client = httpx.Client()

    def _get_access_token(self) -> str:
        """
        Refreshes the access token using the stored refresh token.
        This is called automatically when needed.
        """
        # Spotify's token endpoint requires HTTP Basic auth: base64(client_id:client_secret)
        auth_str = f"{self._client_id}:{self._client_secret}"
        # .encode() → bytes for base64, .decode() → back to str for the HTTP header
        auth_b64 = base64.b64encode(auth_str.encode()).decode()

        response = self._http_client.post(
            "https://accounts.spotify.com/api/token",
            # grant_type "refresh_token" exchanges a long-lived refresh token for a new short-lived access token
            data={"grant_type": "refresh_token", "refresh_token": self._refresh_token},
            headers={"Authorization": f"Basic {auth_b64}"},
        )
        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        print("Successfully refreshed Spotify access token.")
        return self._access_token

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )  # reraise throws the actual underlying error instead of the RetryError
    def get_recently_played(
        self, after_timestamp_unix_ms: int
    ) -> RecentlyPlayedResponse:
        """
        Fetches tracks played after a given timestamp (our cursor).
        """
        if not self._access_token:
            self._access_token = self._get_access_token()

        headers = {"Authorization": f"Bearer {self._access_token}"}
        # limit=50 is the maximum items per request allowed by Spotify's API
        params = {"limit": 50, "after": after_timestamp_unix_ms}

        try:
            response = self._http_client.get(
                "https://api.spotify.com/v1/me/player/recently-played",
                headers=headers,
                params=params,
            )
            # If token expired, get a new one and retry this request once.
            if response.status_code == 401:
                print("Access token expired. Refreshing...")
                self._access_token = self._get_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                response = self._http_client.get(
                    "https://api.spotify.com/v1/me/player/recently-played",
                    headers=headers,
                    params=params,
                )

            response.raise_for_status()

            # Validate the response against our Pydantic schema
            return RecentlyPlayedResponse(**response.json())

        except httpx.HTTPStatusError as e:
            print(f"HTTP error fetching recently played: {e.response.text}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def get_track_by_id(self, track_id: str) -> Optional[dict]:
        """
        Fetches a single track by its Spotify ID.
        Returns the raw track dict, or None if unavailable.
        """
        if not self._access_token:
            self._access_token = self._get_access_token()

        headers = {"Authorization": f"Bearer {self._access_token}"}

        try:
            # Track ID is a path parameter (not query param) per Spotify's REST convention:
            # GET /v1/tracks/{id} returns the full track object for a single track
            response = self._http_client.get(
                f"https://api.spotify.com/v1/tracks/{track_id}",
                headers=headers,
            )
            if response.status_code == 401:
                self._access_token = self._get_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                response = self._http_client.get(
                    f"https://api.spotify.com/v1/tracks/{track_id}",
                    headers=headers,
                )
            if response.status_code == 404:
                return None  # Track removed from Spotify
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(
                f"Spotify /tracks/{track_id} error {e.response.status_code}: {e.response.text}"
            )
            if e.response.status_code == 429:
                print("Rate limited by Spotify. Tenacity will retry...")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )  # reraise throws the actual underlying error instead of the RetryError
    def search_track(self, track_name: str, artist_name: str) -> Optional[dict]:
        """
        Searches Spotify for a specific track and artist.
        Returns the raw track dictionary if found, otherwise None.
        """
        if not self._access_token:
            self._access_token = self._get_access_token()

        headers = {"Authorization": f"Bearer {self._access_token}"}

        # Spotify search uses a structured query syntax: `track:<name> artist:<name>`
        # to scope results to a specific track+artist combination.
        # Quotes/apostrophes are stripped because they break the query parser.
        clean_track = track_name.replace("'", "").replace('"', "")
        clean_artist = artist_name.replace("'", "").replace('"', "")
        query = f"track:{clean_track} artist:{clean_artist}"

        params = {
            "q": query,
            "type": "track",
            "limit": 1,  # We only take the top result
        }

        try:
            response = self._http_client.get(
                "https://api.spotify.com/v1/search",
                headers=headers,
                params=params,
            )

            if response.status_code == 401:
                self._access_token = self._get_access_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                response = self._http_client.get(
                    "https://api.spotify.com/v1/search",
                    headers=headers,
                    params=params,
                )

            response.raise_for_status()
            data = response.json()

            tracks = data.get("tracks", {}).get("items", [])
            if tracks:
                return tracks[0]  # Return the best match
            return None

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:  # Rate limit
                print("Rate limited by Spotify. Tenacity will retry...")
            raise
        except Exception as e:
            print(f"Error searching for {track_name} by {artist_name}: {e}")
            return None
