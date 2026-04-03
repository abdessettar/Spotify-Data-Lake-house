import json
from datetime import datetime, timezone
from azure.core.exceptions import ResourceNotFoundError
from src.config import settings
from src.domain.schemas import PipelineCursor


class StateManager:
    def __init__(self):
        self.blob_service = settings.get_blob_service_client()
        self.container = settings.DATA_CONTAINER
        self.cursor_path = "system/state/cursor.json"

    def get_cursor(self) -> PipelineCursor:
        """
        Downloads the cursor from Blob Storage.
        If not found, returns a default cursor.
        """
        blob_client = self.blob_service.get_blob_client(
            container=self.container, blob=self.cursor_path
        )

        try:
            download_stream = blob_client.download_blob()
            data = json.loads(download_stream.readall())
            print(f"Loaded cursor: {data}")
            return PipelineCursor(**data)
        except ResourceNotFoundError:
            print("No cursor found. Starting from scratch.")
            # Default to 2017: Spotify's "recently played" endpoint only goes back ~50 tracks,
            # but using an old date ensures we capture everything on the very first run
            return PipelineCursor(
                last_run_timestamp=datetime(2017, 1, 1, tzinfo=timezone.utc),
                last_played_at_unix_ms=0,
            )

    def update_cursor(self, new_cursor: PipelineCursor):
        """
        Overwrites the cursor in Blob Storage.
        """
        blob_client = self.blob_service.get_blob_client(
            container=self.container, blob=self.cursor_path
        )

        # model_dump_json() serializes the Pydantic model directly to a JSON string,
        # handling datetime → ISO 8601 conversion automatically
        data = new_cursor.model_dump_json()
        blob_client.upload_blob(data, overwrite=True)
        print(f"State saved: {data}")
