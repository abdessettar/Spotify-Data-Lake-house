import json
from datetime import datetime
from typing import List
from src.config import settings
from src.domain.schemas import PlayedItem


class DataLakeConnector:
    """
    Handles all read/write operations to the Data Lake (Azure Blob Storage).
    """

    def __init__(self):
        self.blob_service = settings.get_blob_service_client()
        self.container = settings.DATA_CONTAINER

    def save_raw_played_items(self, items: List[PlayedItem], ingestion_time: datetime):
        """
        Saves a list of played items as a single JSON file in the Bronze layer.
        The path is partitioned by date.
        """
        if not items:
            return

        # Timestamp-based filename ensures uniqueness across runs (no overwrites)
        filename = f"{ingestion_time.strftime('%Y-%m-%d-%H%M%S')}.json"

        # Hive-style partition path (year=/month=/day=)
        path = (
            f"bronze/spotify_api/recently_played/"
            f"year={ingestion_time.year}/"
            f"month={ingestion_time.month:02d}/"
            f"day={ingestion_time.day:02d}/{filename}"
        )

        blob_client = self.blob_service.get_blob_client(
            container=self.container, blob=path
        )

        # model_dump(mode='json') converts datetime objects and other non-JSON types
        # into JSON-safe primitives (e.g., datetime → ISO 8601 string)
        data_to_save = [item.model_dump(mode="json") for item in items]

        # Upload the data
        blob_client.upload_blob(json.dumps(data_to_save, indent=2), overwrite=True)
        print(f"Successfully saved {len(items)} raw items to '{path}'")

    def save_backfill_file(self, filename: str, content: bytes):
        """
        Saves a raw Spotify export JSON file into the Bronze export folder.
        """
        path = f"bronze/spotify_export/{filename}"

        blob_client = self.blob_service.get_blob_client(
            container=self.container, blob=path
        )
        blob_client.upload_blob(content, overwrite=True)
        print(f"Successfully uploaded backfill file to '{path}'")

    def list_bronze_files(self, source: str) -> List[str]:
        """
        Lists all file paths in the Bronze layer for a specific source. Source should be 'spotify_api' or 'spotify_export'.
        Returns paths formatted for Polars (az://container/path).
        """
        prefix = f"bronze/{source}/"
        container_client = self.blob_service.get_container_client(self.container)

        paths = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            if blob.name.endswith(".json"):
                # az:// URI scheme is the convention for fsspec/adlfs and Polars
                # to address blobs: az://<container>/<blob_path>
                paths.append(f"az://{self.container}/{blob.name}")

        return paths

    def upload_bytes(self, data: bytes, destination_path: str):
        """Uploads raw bytes to a specific path in the data lake."""
        blob_client = self.blob_service.get_blob_client(
            container=self.container, blob=destination_path
        )
        blob_client.upload_blob(data, overwrite=True)
        print(
            f"Successfully uploaded {len(data) / 1024:.2f} KB to '{destination_path}'"
        )
