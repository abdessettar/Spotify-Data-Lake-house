import json
import time
import base64
import polars as pl

from src.config import settings
from src.connectors.storage import DataLakeConnector
from src.connectors.lastfm import LastfmClient


class LastfmEnrichmentPipeline:
    def __init__(self):
        self.storage = DataLakeConnector()
        self.lastfm = LastfmClient()
        self.storage_options = settings.polars_storage_options
        self.fsspec_options = settings.fsspec_storage_options

    def _list_silver_parquet_files(self, prefix: str) -> list:
        """Lists Silver parquet files explicitly (glob doesn't work reliably over az://)."""
        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )
        return [
            f"az://{self.storage.container}/{blob.name}"
            for blob in container_client.list_blobs(name_starts_with=prefix)
            if blob.name.endswith(".parquet")
        ]

    def run(self, batch_size: int = 200):
        print("--- Starting Last.fm Enrichment Pipeline ---")

        # 1. Read (track_id, track_name, artist_name) from the Silver fact table.
        #    The fact table is the only Silver table that carries both track_id and
        #    artist_name together, so it's the most convenient source for the lookup keys.
        event_files = self._list_silver_parquet_files("silver/listening_events/")
        if not event_files:
            print("No Silver event files found. Run the transform pipeline first.")
            return

        df = (
            pl.scan_parquet(
                event_files,
                storage_options=self.storage_options,
                hive_partitioning=True,
            )
            .select(["track_id", "track_name", "artist_name"])
            .filter(pl.col("track_id").is_not_null())
            .unique(subset=["track_id"])
            .collect()
        )

        print(f"Found {df.height} unique tracks with IDs in Silver.")

        # 2. Decode filenames of existing Bronze files to find already attempted track_ids.
        existing_files = self.storage.list_bronze_files("lastfm_enrichment")
        already_fetched: set = set()
        for f in existing_files:
            filename = f.split("/")[-1].replace(".json", "")
            try:
                already_fetched.add(base64.b64decode(filename).decode("utf-8"))
            except Exception:
                pass

        # 3. Filter to tracks not yet attempted, then cap at batch_size.
        to_fetch = df.filter(~pl.col("track_id").is_in(list(already_fetched))).head(
            batch_size
        )

        print(
            f"Tracks to fetch: {to_fetch.height} "
            f"(skipping {len(already_fetched)} already attempted)"
        )

        # 4. Fetch each track and save the result to Bronze.
        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )

        if to_fetch.is_empty():
            print("All tracks have already been attempted. Skipping to artist phase.")
        found_count = 0

        for row in to_fetch.iter_rows(named=True):
            track_id = row["track_id"]
            track_name = row["track_name"]
            artist_name = row["artist_name"]

            safe_name = base64.b64encode(track_id.encode("utf-8")).decode("utf-8")
            blob_path = f"bronze/lastfm_enrichment/{safe_name}.json"

            result = self.lastfm.get_track_info(track_name, artist_name)

            if result:
                record = {"track_id": track_id, **result}
                container_client.upload_blob(
                    name=blob_path, data=json.dumps(record, indent=2), overwrite=True
                )
                found_count += 1
                print(f"Fetched: {track_name} — tags: {result['tags']}")
            else:
                container_client.upload_blob(
                    name=blob_path, data=json.dumps({"not_found": True}), overwrite=True
                )

            time.sleep(1)

        print(
            f"--- Last.fm Track Enrichment Complete: {found_count}/{to_fetch.height} found ---"
        )

        # Artist tags
        # Artist level tags are much better populated than track level tags on Last.fm.
        # We read artist_id + artist_name from the Silver artists dimension and fetch
        # top tags for each artist not yet attempted.
        print("\n--- Starting Last.fm Artist Tag Enrichment ---")

        az_artists_path = f"az://{self.storage.container}/silver/artists/data.parquet"
        try:
            df_artists = pl.read_parquet(
                az_artists_path, storage_options=self.storage_options
            )
        except Exception:
            print("No Silver artists file found. Run transform first.")
            return

        print(f"Found {df_artists.height} artists in Silver.")

        existing_artist_files = self.storage.list_bronze_files(
            "lastfm_artist_enrichment"
        )
        already_fetched_artists: set = set()
        for f in existing_artist_files:
            filename = f.split("/")[-1].replace(".json", "")
            try:
                already_fetched_artists.add(base64.b64decode(filename).decode("utf-8"))
            except Exception:
                pass

        artists_to_fetch = df_artists.filter(
            ~pl.col("artist_id").is_in(list(already_fetched_artists))
        ).head(batch_size)

        print(
            f"Artists to fetch: {artists_to_fetch.height} "
            f"(skipping {len(already_fetched_artists)} already attempted)"
        )

        artist_found_count = 0
        for row in artists_to_fetch.iter_rows(named=True):
            artist_id = row["artist_id"]
            artist_name = row["artist_name"]

            safe_name = base64.b64encode(artist_id.encode("utf-8")).decode("utf-8")
            blob_path = f"bronze/lastfm_artist_enrichment/{safe_name}.json"

            result = self.lastfm.get_artist_tags(artist_name)

            if result:
                record = {"artist_id": artist_id, **result}
                container_client.upload_blob(
                    name=blob_path, data=json.dumps(record, indent=2), overwrite=True
                )
                artist_found_count += 1
                print(f"Fetched: {artist_name} — tags: {result['tags']}")
            else:
                container_client.upload_blob(
                    name=blob_path, data=json.dumps({"not_found": True}), overwrite=True
                )

            time.sleep(1)

        print(
            f"--- Last.fm Artist Enrichment Complete: {artist_found_count}/{artists_to_fetch.height} found ---"
        )
