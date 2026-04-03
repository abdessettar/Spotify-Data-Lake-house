import json
import time
import base64
import httpx
import polars as pl

from src.config import settings
from src.connectors.storage import DataLakeConnector
from src.connectors.spotify import SpotifyClient
from src.domain.schemas import SpotifyTrack


class EnrichmentPipeline:
    def __init__(self):
        self.storage = DataLakeConnector()
        self.spotify = SpotifyClient()
        self.storage_options = settings.polars_storage_options
        self.fsspec_options = settings.fsspec_storage_options

    def _list_silver_parquet_files(self, prefix: str) -> list:
        """Lists Silver parquet files explicitly (glob doesn't work reliably over az://).

        We enumerate blobs via the Azure SDK instead of using Polars' glob (az://*.parquet)
        because Polars' fsspec-based glob over Azure Blob Storage is unreliable — it can
        miss files or fail silently depending on the storage backend and fsspec version.
        """
        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )
        return [
            f"az://{self.storage.container}/{blob.name}"
            for blob in container_client.list_blobs(name_starts_with=prefix)
            if blob.name.endswith(".parquet")
        ]

    def run(self, batch_size: int = 50):
        print("--- Starting Catalog Enrichment Pipeline ---")

        # 1. Find track_ids in the fact table that are not in the dimension table
        event_files = self._list_silver_parquet_files("silver/listening_events/")
        if not event_files:
            print("No Silver event files found. Run the transform pipeline first.")
            return

        print(f"Reading {len(event_files)} Silver event files...")
        df_events = (
            pl.scan_parquet(
                event_files,
                storage_options=self.storage_options,
                hive_partitioning=True,
            )
            .select("track_id")
            .filter(pl.col("track_id").is_not_null())
            .unique()
            .collect()
        )

        fact_track_ids = set(df_events["track_id"].to_list())
        print(f"Found {len(fact_track_ids)} unique track IDs in the fact table.")

        # 2. Read existing dimension track_ids
        dim_files = self._list_silver_parquet_files("silver/tracks/")
        known_track_ids: set = set()
        if dim_files:
            df_dim = (
                pl.scan_parquet(dim_files, storage_options=self.storage_options)
                .select("track_id")
                .collect()
            )
            known_track_ids = set(df_dim["track_id"].to_list())
            print(
                f"Found {len(known_track_ids)} track IDs already in the dimension table."
            )

        # 3. Find track_ids missing from dimension table
        # Set difference: track_ids referenced in facts but absent from dimension tables.
        missing_ids = fact_track_ids - known_track_ids
        print(f"Found {len(missing_ids)} track IDs missing from the dimension table.")

        if not missing_ids:
            print("No missing track IDs found. The catalog is fully enriched!! :)")
            return

        # 4. Exclude track_ids already attempted in Bronze enrichment
        existing_files = self.storage.list_bronze_files("spotify_enrichment")
        already_fetched: set = set()
        for f in existing_files:
            filename = f.split("/")[-1].replace(".json", "")
            # Filenames are base64-encoded track_ids. Decode to recover the original ID.
            # Old files used "artist||track" format (older search-based approach b4 dumps), but;
            # we skip those by checking for "||", only plain track_ids are relevant now.
            # Do not delete old files, they may be neededed (Spotify API ran forever for those
            # so better not lose them).
            try:
                decoded = base64.b64decode(filename).decode("utf-8")
                if (
                    "||" not in decoded
                ):  # It's a plain track_id, not the old artist||track format
                    already_fetched.add(decoded)
            except Exception:
                pass

        to_fetch = list(missing_ids - already_fetched)
        print(
            f"Track IDs remaining to fetch: {len(to_fetch)} (skipping {len(already_fetched)} already attempted)"
        )

        if not to_fetch:
            print("All missing tracks have already been attempted.")
            return

        # 5. Fetch tracks one by one
        to_fetch = to_fetch[:batch_size]
        print(f"Fetching {len(to_fetch)} tracks from Spotify API...")

        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )
        found_count = 0

        for track_id in to_fetch:
            # Base64-encode the track_id to create a filesystem-safe blob name:
            # track_ids may contain characters that are problematic in blob paths.
            safe_name = base64.b64encode(track_id.encode("utf-8")).decode("utf-8")
            blob_path = f"bronze/spotify_enrichment/{safe_name}.json"

            try:
                track_data = self.spotify.get_track_by_id(track_id)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    retry_after = e.response.headers.get("Retry-After", "unknown")
                    print(
                        f"HARD RATE LIMIT HIT. Spotify says wait {retry_after}s. Stopping..."
                    )
                    break
                print(
                    f"HTTP {e.response.status_code} for {track_id}. Marking as not found."
                )
                container_client.upload_blob(
                    name=blob_path, data=json.dumps({"not_found": True}), overwrite=True
                )
                continue

            if track_data:
                try:
                    valid_track = SpotifyTrack(**track_data)
                    # Store search_artist_name and search_track_name alongside the track data.
                    # The Silver pipeline uses these fields to join enrichment results back to
                    # export events that lack track_ids (matching on artist_name + track_name).
                    record = {
                        "search_artist_name": valid_track.artists[0].name
                        if valid_track.artists
                        else None,
                        "search_track_name": valid_track.name,
                        "track": valid_track.model_dump(mode="json"),
                    }
                    container_client.upload_blob(
                        name=blob_path,
                        data=json.dumps(record, indent=2),
                        overwrite=True,
                    )
                    found_count += 1
                    print(f"Fetched: {valid_track.name}")
                except Exception as e:
                    print(f"Validation failed for {track_id}: {e}")
                    container_client.upload_blob(
                        name=blob_path,
                        data=json.dumps({"not_found": True}),
                        overwrite=True,
                    )
            else:
                container_client.upload_blob(
                    name=blob_path, data=json.dumps({"not_found": True}), overwrite=True
                )

            # time.sleep(random.uniform(25, 30))
            time.sleep(35)

        print(
            f"--- Enrichment Batch Complete: Fetched {found_count} out of {len(to_fetch)} ---"
        )
