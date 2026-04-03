from datetime import datetime
from src.core.state import StateManager
from src.connectors.spotify import SpotifyClient
from src.connectors.storage import DataLakeConnector
from src.domain.schemas import PipelineCursor


class IngestionPipeline:
    def __init__(self):
        self.state_manager = StateManager()
        self.spotify_client = SpotifyClient()
        self.storage_connector = DataLakeConnector()

    def run(self):
        print("--- Starting Ingestion Pipeline ---")
        ingestion_time = datetime.now()

        # 1. Get current state
        cursor = self.state_manager.get_cursor()

        # 2. Fetch new data from Spotify
        print(f"Fetching data after timestamp: {cursor.last_played_at_unix_ms}")
        response = self.spotify_client.get_recently_played(
            after_timestamp_unix_ms=cursor.last_played_at_unix_ms
        )

        if not response.items:
            print("No new tracks found. Pipeline run finished.")
            return

        print(f"Fetched {len(response.items)} new tracks from Spotify.")

        # 3. Save raw data to Bronze layer
        self.storage_connector.save_raw_played_items(response.items, ingestion_time)

        # 4. Update the cursor state
        # The Spotify API returns items in desc chronological order (newest first),
        # so items[0] is always the most recently played track in the batch.
        most_recent_item = response.items[0]
        newest_played_at_str = most_recent_item.played_at

        # Parse the ISO string back to a datetime object:
        # Spotify timestamps end with 'Z' (Zulu time / UTC), but fromisoformat()
        # doesn't recognize the 'Z' suffix. replace with '+00:00' for compat.
        newest_played_at = datetime.fromisoformat(
            newest_played_at_str.replace("Z", "+00:00")
        )

        # Convert to Unix milliseconds (seconds * 1000) because Spotify's `after` param
        # expects a Unix timestamp in milliseconds, not seconds.
        new_cursor_timestamp_ms = int(newest_played_at.timestamp() * 1000)

        new_cursor = PipelineCursor(
            last_run_timestamp=ingestion_time,
            last_played_at_unix_ms=new_cursor_timestamp_ms,
        )

        self.state_manager.update_cursor(new_cursor)

        print("--- Ingestion Pipeline Finished Successfully ---")
