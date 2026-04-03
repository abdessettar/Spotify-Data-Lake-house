# Using ruff made this file longer than desired, so it is hard to follow a bit.
# This also applies to gold.py.
# TODO: Refactor into smaller functions and/or multiple files to improve readability and maintainability.

import json
import io
import polars as pl
import fsspec
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import settings
from src.connectors.storage import DataLakeConnector


class SilverPipeline:
    def __init__(self):
        self.storage = DataLakeConnector()
        self.fsspec_options = settings.fsspec_storage_options

    def _process_enrichment_data(self):
        """Reads Bronze enrichment files, returning mapping & dimension data."""
        files = self.storage.list_bronze_files("spotify_enrichment")
        if not files:
            return pl.DataFrame(), pl.DataFrame()

        print(f"Loading {len(files)} enriched tracks from Bronze...")
        blob_names = [f.split(f"{self.storage.container}/", 1)[1] for f in files]
        records = self._download_blobs_parallel(blob_names)

        mapping_records = []
        track_records = []
        for data in records:
            # Skip "not_found" marker files as these are tracks that Spotify's API
            # couldn't locate (deleted tracks, region restricted, etc.).
            mapping_records.append(
                {
                    "search_artist_name": data["search_artist_name"],
                    "search_track_name": data["search_track_name"],
                    "enriched_track_id": data["track"]["id"],
                }
            )
            track_records.append({"track": data["track"]})

        df_mapping = (
            pl.DataFrame(mapping_records) if mapping_records else pl.DataFrame()
        )

        if track_records:
            # Convert list of track dicts to in memory JSON bytes, then parse with Polars.
            # This lets Polars infer the nested struct schema from the full track objects.
            df_enriched_tracks = pl.read_json(
                json.dumps(track_records).encode("utf-8"), infer_schema_length=None
            )
        else:
            df_enriched_tracks = pl.DataFrame()

        return df_mapping, df_enriched_tracks

    def _download_blobs_parallel(
        self, blob_names: list[str], max_workers: int = 64
    ) -> list[dict]:
        """Downloads a list of blob names in parallel and returns non empty, non-not_found records."""
        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )

        def _fetch(blob_name: str):
            try:
                raw = container_client.download_blob(blob_name).readall()
                data = json.loads(raw)
                if not data.get("not_found"):
                    return data
            except Exception:
                pass
            return None

        records = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_fetch, name): name for name in blob_names}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    records.append(result)
        return records

    def _process_lastfm_data(self) -> pl.DataFrame:
        """Reads Bronze Last.fm enrichment files and returns a flat Silver dimension DataFrame."""
        files = self.storage.list_bronze_files("lastfm_enrichment")
        if not files:
            return pl.DataFrame()

        print(f"Loading {len(files)} Last.fm enrichment files from Bronze...")
        blob_names = [f.split(f"{self.storage.container}/", 1)[1] for f in files]
        records = self._download_blobs_parallel(blob_names)

        if not records:
            return pl.DataFrame()

        return pl.DataFrame(records).with_columns(
            [
                pl.col("listeners").cast(pl.Int64),
                pl.col("playcount").cast(pl.Int64),
                pl.col("tags").cast(pl.List(pl.Utf8)),
            ]
        )

    def _process_lastfm_artist_data(self) -> pl.DataFrame:
        """Reads Bronze Last.fm artist enrichment files and returns a Silver dimension DF."""
        files = self.storage.list_bronze_files("lastfm_artist_enrichment")
        if not files:
            return pl.DataFrame()

        print(f"Loading {len(files)} Last.fm artist tag files from Bronze...")
        blob_names = [f.split(f"{self.storage.container}/", 1)[1] for f in files]
        records = self._download_blobs_parallel(blob_names)

        if not records:
            return pl.DataFrame()

        return pl.DataFrame(records).with_columns(
            [
                pl.col("tags").cast(pl.List(pl.Utf8)),
            ]
        )

    def _extract_dimensions(self, df: pl.DataFrame):
        """Helper to extract and strictly type flat dimension tables from nested JSON."""
        if df.is_empty() or "track" not in df.columns:
            return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

        # Unnest artists: track.artists is a list of structs (one per artist on the track).
        # explode() flattens the list so each artist gets its own row for deduplication.
        df_artists = (
            df.select(
                pl.col("track").struct.field("artists").list.explode().alias("artist")
            )
            .select(
                [
                    pl.col("artist").struct.field("id").alias("artist_id"),
                    pl.col("artist").struct.field("name").alias("artist_name"),
                    pl.col("artist").struct.field("uri").alias("artist_uri"),
                ]
            )
            .drop_nulls("artist_id")
            .unique(subset=["artist_id"])
        )

        df_albums = (
            df.select(
                [
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("id")
                    .alias("album_id"),
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("name")
                    .alias("album_name"),
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("album_type")
                    .alias("album_type"),
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("release_date")
                    .alias("release_date"),
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("total_tracks")
                    .cast(pl.Int64)
                    .alias("total_tracks"),
                    # images is a list of {url, height, width} structs sorted by size (largest first).
                    # Take the first (largest) image's URL as the album cover art if we ever need it.
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("images")
                    .list.first()
                    .struct.field("url")
                    .alias("image_url"),
                ]
            )
            .drop_nulls("album_id")
            .unique(subset=["album_id"])
        )

        # Explicit casts on numeric/boolean fields prevent Polars SchemaError when
        # concatenating DataFrames where one has Int64 and another has Null type for the
        # same column (e.g., when a batch has all-null popularity values).
        #
        # Important note:
        # external_ids which contains isrc is only presentt in responses from
        # /v1/tracks/{id} (enrichment), not from /v1/me/player/recently-played (API ingest).
        # Check the struct schema before selecting to avoid StructFieldNotFoundError.
        # Check two levels deep: external_ids must exist on track AND isrc must exist
        # inside external_ids. With unified schema inference across all files, external_ids
        # can appear as a zero-field struct (from records where it was an empty object {}),
        # which means the outer check passes but .struct.field("isrc") still raises.
        track_dtype = df.schema["track"]
        track_field_names = [f.name for f in track_dtype.fields]
        has_isrc = False
        if "external_ids" in track_field_names:
            ext_ids_dtype = next(
                f.dtype for f in track_dtype.fields if f.name == "external_ids"
            )
            has_isrc = isinstance(ext_ids_dtype, pl.Struct) and any(
                f.name == "isrc" for f in ext_ids_dtype.fields
            )
        isrc_expr = (
            pl.col("track")
            .struct.field("external_ids")
            .struct.field("isrc")
            .alias("isrc")
            if has_isrc
            else pl.lit(None).cast(pl.Utf8).alias("isrc")
        )
        df_tracks = (
            df.select(
                [
                    pl.col("track").struct.field("id").alias("track_id"),
                    pl.col("track").struct.field("name").alias("track_name"),
                    pl.col("track")
                    .struct.field("duration_ms")
                    .cast(pl.Int64)
                    .alias("duration_ms"),
                    pl.col("track")
                    .struct.field("explicit")
                    .cast(pl.Boolean)
                    .alias("explicit"),
                    pl.col("track")
                    .struct.field("popularity")
                    .cast(pl.Int64)
                    .alias("popularity"),
                    pl.col("track")
                    .struct.field("track_number")
                    .cast(pl.Int64)
                    .alias("track_number"),
                    pl.col("track")
                    .struct.field("album")
                    .struct.field("id")
                    .alias("album_id"),
                    pl.col("track").struct.field("uri").alias("track_uri"),
                    isrc_expr,
                ]
            )
            .drop_nulls("track_id")
            .unique(subset=["track_id"])
        )

        return df_artists, df_albums, df_tracks

    def _read_existing_silver_table(self, blob_path: str) -> pl.DataFrame:
        """Downloads an existing Silver table if it exists, returns empty DataFrame otherwise."""
        try:
            container_client = self.storage.blob_service.get_container_client(
                self.storage.container
            )
            raw = container_client.download_blob(blob_path).readall()
            return pl.read_parquet(io.BytesIO(raw))
        except Exception:
            return pl.DataFrame()

    def _preserve_enrichment_columns(
        self, df_new: pl.DataFrame, blob_path: str, id_col: str
    ) -> pl.DataFrame:
        """
        Re-joins any columns present in the existing Silver table but absent from df_new.
        Prevents the transform pipeline from wiping out dump-enriched columns (genres,
        followers_total, label, disc_number, etc.) on each run.
        """
        df_existing = self._read_existing_silver_table(blob_path)
        if df_existing.is_empty():
            return df_new
        extra_cols = [c for c in df_existing.columns if c not in df_new.columns]
        if not extra_cols:
            return df_new
        return df_new.join(
            df_existing.select([id_col] + extra_cols), on=id_col, how="left"
        )

    def run(self):
        print("--- Starting Silver Star-Schema Pipeline ---")

        api_files = self.storage.list_bronze_files("spotify_api")
        export_files = self.storage.list_bronze_files("spotify_export")

        df_api_raw = pl.DataFrame()
        if api_files:
            # Collect all records from all API files into one list before parsing.
            # Parsing per-file then pl.concat(how="diagonal") fails when the `track`
            # struct has different numbers of fields across files (Spotify API response
            # shape can change over time). A single pl.read_json call with
            # infer_schema_length=None inspects every record and builds a unified struct
            # schema, filling missing fields with null.
            all_api_records = []
            for f in api_files:
                with fsspec.open(f, "rb", **self.fsspec_options) as file_obj:
                    records = json.loads(file_obj.read())
                    if isinstance(records, list):
                        all_api_records.extend(records)
            if all_api_records:
                df_api_raw = pl.read_json(
                    json.dumps(all_api_records).encode("utf-8"),
                    infer_schema_length=None,
                )

        df_mapping, df_enriched_tracks = self._process_enrichment_data()

        # 1. BUILD AND WRITE DIMENSIONS
        print("Extracting and writing Dimensions (Artists, Albums, Tracks)...")
        api_artists, api_albums, api_tracks = self._extract_dimensions(df_api_raw)
        enrich_artists, enrich_albums, enrich_tracks = self._extract_dimensions(
            df_enriched_tracks
        )

        # Merge and Write Artists
        dim_artists_list = [
            df for df in [api_artists, enrich_artists] if not df.is_empty()
        ]
        if dim_artists_list:
            df_artists = pl.concat(dim_artists_list, how="diagonal").unique(
                subset=["artist_id"]
            )
            df_artists = self._preserve_enrichment_columns(
                df_artists, "silver/artists/data.parquet", "artist_id"
            )
            with io.BytesIO() as buffer:
                df_artists.write_parquet(buffer)
                self.storage.upload_bytes(
                    buffer.getvalue(), "silver/artists/data.parquet"
                )

        # Merge and Write Albums
        dim_albums_list = [
            df for df in [api_albums, enrich_albums] if not df.is_empty()
        ]
        if dim_albums_list:
            df_albums = pl.concat(dim_albums_list, how="diagonal").unique(
                subset=["album_id"]
            )
            df_albums = self._preserve_enrichment_columns(
                df_albums, "silver/albums/data.parquet", "album_id"
            )
            with io.BytesIO() as buffer:
                df_albums.write_parquet(buffer)
                self.storage.upload_bytes(
                    buffer.getvalue(), "silver/albums/data.parquet"
                )

        # Merge and Write Tracks
        dim_tracks_list = [
            df for df in [api_tracks, enrich_tracks] if not df.is_empty()
        ]
        if dim_tracks_list:
            df_tracks = pl.concat(dim_tracks_list, how="diagonal").unique(
                subset=["track_id"]
            )
            df_tracks = self._preserve_enrichment_columns(
                df_tracks, "silver/tracks/data.parquet", "track_id"
            )
            with io.BytesIO() as buffer:
                df_tracks.write_parquet(buffer)
                self.storage.upload_bytes(
                    buffer.getvalue(), "silver/tracks/data.parquet"
                )

        # Write Last.fm track dimension
        df_lastfm = self._process_lastfm_data()
        if not df_lastfm.is_empty():
            with io.BytesIO() as buffer:
                df_lastfm.write_parquet(buffer)
                self.storage.upload_bytes(
                    buffer.getvalue(), "silver/lastfm/data.parquet"
                )

        # Write Last.fm artist tags dimension
        df_lastfm_artists = self._process_lastfm_artist_data()
        if not df_lastfm_artists.is_empty():
            with io.BytesIO() as buffer:
                df_lastfm_artists.write_parquet(buffer)
                self.storage.upload_bytes(
                    buffer.getvalue(), "silver/lastfm_artists/data.parquet"
                )

        # 2. PROCESS EVENTS (API)
        df_api_events = pl.DataFrame()
        if not df_api_raw.is_empty():
            df_api_events = df_api_raw.select(
                [
                    # Polars' str.to_datetime() doesn't handle the "Z" UTC suffix natively,
                    # so we strip it first. The resulting datetime is implicitly UTC.
                    pl.col("played_at")
                    .str.replace("Z", "")
                    .str.to_datetime()
                    .alias("played_at_utc"),
                    pl.col("track").struct.field("id").alias("track_id"),
                    pl.col("track").struct.field("name").alias("track_name"),
                    pl.col("track")
                    .struct.field("artists")
                    .list.first()
                    .struct.field("name")
                    .alias("artist_name"),
                    pl.col("track")
                    .struct.field("duration_ms")
                    .alias("duration_ms")
                    .cast(pl.Int64),
                    pl.col("context").struct.field("type").alias("context_type"),
                    pl.col("context").struct.field("uri").alias("context_uri"),
                    pl.lit("api").alias("source_type"),
                    pl.lit(None).cast(pl.Utf8).alias("platform"),
                    pl.lit(None).cast(pl.Utf8).alias("conn_country"),
                    pl.lit(None).cast(pl.Utf8).alias("reason_start"),
                    pl.lit(None).cast(pl.Utf8).alias("reason_end"),
                    pl.lit(None).cast(pl.Boolean).alias("shuffle"),
                    pl.lit(None).cast(pl.Boolean).alias("skipped"),
                    pl.lit(None).cast(pl.Boolean).alias("offline"),
                    pl.lit(None).cast(pl.Boolean).alias("incognito_mode"),
                    pl.lit(None).cast(pl.Utf8).alias("album_name"),
                ]
            )

        # 3. PROCESS EVENTS (EXPORT + MAPPING)
        df_export_events = pl.DataFrame()
        if export_files:
            export_dfs = []
            for f in export_files:
                with fsspec.open(f, "r", **self.fsspec_options) as file_obj:
                    records = json.load(file_obj)
                    # infer_schema_length=None forces Polars to scan ALL rows to infer types.
                    # Needed because some columns (e.g., offline_timestamp) can be null in early
                    # rows but integer later, so partial inference would mistype them.
                    df = pl.DataFrame(records, infer_schema_length=None)
                    # Cast any columns inferred as Null type to string (ie Utf8). When concating
                    # multiple files with how="diagonal", Polars requires matching types: a column
                    # that's all-null in one file (Null type) would clash with Utf8 in another.
                    df = df.cast(
                        {
                            col: pl.Utf8
                            for col, dtype in df.schema.items()
                            if dtype == pl.Null
                        }
                    )
                    export_dfs.append(df)
            df_export_raw = pl.concat(export_dfs, how="diagonal")

            # Detect export format by checking for a column unique to the extended format.
            # Extended exports contain "spotify_track_uri"; standard exports do not.
            is_extended = "spotify_track_uri" in df_export_raw.columns
            played_at_col = "ts" if "ts" in df_export_raw.columns else "endTime"
            artist_col = (
                "master_metadata_album_artist_name"
                if "master_metadata_album_artist_name" in df_export_raw.columns
                else "artistName"
            )
            track_col = (
                "master_metadata_track_name"
                if "master_metadata_track_name" in df_export_raw.columns
                else "trackName"
            )
            duration_col = (
                "ms_played" if "ms_played" in df_export_raw.columns else "msPlayed"
            )

            if played_at_col == "endTime":
                played_at_expr = (
                    pl.col(played_at_col)
                    .str.to_datetime(format="%Y-%m-%d %H:%M")
                    .alias("played_at_utc")
                )
            else:
                played_at_expr = (
                    pl.col(played_at_col)
                    .str.replace("Z", "")
                    .str.to_datetime(strict=False)
                    .alias("played_at_utc")
                )

            # Core columns shared by both formats
            select_exprs = [
                played_at_expr,
                pl.col(track_col).alias("track_name"),
                pl.col(artist_col).alias("artist_name"),
                pl.col(duration_col).alias("duration_ms").cast(pl.Int64),
                pl.lit(None).cast(pl.Utf8).alias("context_type"),
                pl.lit(None).cast(pl.Utf8).alias("context_uri"),
            ]

            if is_extended:
                # Parse the Spotify URI format "spotify:track:XXXXX" to extract just the track ID.
                # Guard against nulls and non-track URIs (e.g., podcast episodes).
                select_exprs.append(
                    pl.when(
                        pl.col("spotify_track_uri").is_not_null()
                        & pl.col("spotify_track_uri").str.starts_with("spotify:track:")
                    )
                    .then(pl.col("spotify_track_uri").str.split(":").list.last())
                    .otherwise(pl.lit(None).cast(pl.Utf8))
                    .alias("track_id")
                )
                select_exprs.append(pl.lit("extended_export").alias("source_type"))

                # Behavioral columns from extended export (absent from standard export). Check for each column's existence to avoid errors.
                for col_name in [
                    "platform",
                    "conn_country",
                    "reason_start",
                    "reason_end",
                ]:
                    if col_name in df_export_raw.columns:
                        select_exprs.append(pl.col(col_name).cast(pl.Utf8))
                    else:
                        select_exprs.append(pl.lit(None).cast(pl.Utf8).alias(col_name))
                for col_name in ["shuffle", "skipped", "offline", "incognito_mode"]:
                    if col_name in df_export_raw.columns:
                        select_exprs.append(pl.col(col_name).cast(pl.Boolean))
                    else:
                        select_exprs.append(
                            pl.lit(None).cast(pl.Boolean).alias(col_name)
                        )

                # Album name from extended export
                if "master_metadata_album_album_name" in df_export_raw.columns:
                    select_exprs.append(
                        pl.col("master_metadata_album_album_name")
                        .cast(pl.Utf8)
                        .alias("album_name")
                    )
                else:
                    select_exprs.append(pl.lit(None).cast(pl.Utf8).alias("album_name"))
            else:
                # Standard export: no track_id, no behavioral data
                select_exprs.append(pl.lit(None).cast(pl.Utf8).alias("track_id"))
                select_exprs.append(pl.lit("export").alias("source_type"))
                for col_name in [
                    "platform",
                    "conn_country",
                    "reason_start",
                    "reason_end",
                ]:
                    select_exprs.append(pl.lit(None).cast(pl.Utf8).alias(col_name))
                for col_name in ["shuffle", "skipped", "offline", "incognito_mode"]:
                    select_exprs.append(pl.lit(None).cast(pl.Boolean).alias(col_name))
                select_exprs.append(pl.lit(None).cast(pl.Utf8).alias("album_name"))

            df_export_events = df_export_raw.select(select_exprs).drop_nulls(
                subset=["track_name"]
            )

            # Enrich missing track_ids using the enrichment mapping table. Joins export events
            # to enrichment results on (artist_name, track_name). Then coalesces: using the
            # existing track_id if present, otherwise fills from the enriched_track_id.
            # This fills gaps where the export had no spotify_track_uri but we later
            # fetched the track via the enrichment pipeline.
            if not df_mapping.is_empty():
                df_export_events = (
                    df_export_events.join(
                        df_mapping,
                        left_on=["artist_name", "track_name"],
                        right_on=["search_artist_name", "search_track_name"],
                        how="left",
                    )
                    .with_columns(
                        [
                            pl.coalesce(["track_id", "enriched_track_id"]).alias(
                                "track_id"
                            )
                        ]
                    )
                    .drop("enriched_track_id")
                )

        # 4. COMBINE EVENTS AND WRITE FACT TABLE
        print("Combining and writing Fact Table (Events)...")
        event_dfs = []
        if not df_api_events.is_empty():
            event_dfs.append(df_api_events)
        if not df_export_events.is_empty():
            event_dfs.append(df_export_events)
        if not event_dfs:
            print("No events to process.")
            return

        df_events = pl.concat(event_dfs, how="diagonal")
        # Dedup priority: api (1, richest context) > extended_export (2, behavioral) > export (3, sparse).
        # When two sources report the same track at the same time, we keep the highest-priority one.
        # Sorting by [played_at_utc, _source_priority] ascending then keeping "first" per
        # (played_at_utc, track_name) group ensures the lowest priority number (best source) wins.
        source_priority = {"api": 1, "extended_export": 2, "export": 3}
        df_events = (
            df_events.with_columns(
                pl.col("source_type")
                .replace_strict(source_priority)
                .alias("_source_priority")
            )
            .sort(["played_at_utc", "_source_priority"], descending=[False, False])
            .unique(subset=["played_at_utc", "track_name"], keep="first")
            .drop("_source_priority")
            .with_columns(
                [
                    pl.col("played_at_utc").dt.year().alias("year"),
                    pl.col("played_at_utc").dt.month().alias("month"),
                ]
            )
        )

        # Write partitioned data by iterating through groups
        for (year, month), partition_df in df_events.group_by(["year", "month"]):
            path = f"silver/listening_events/year={year}/month={month}/data.parquet"
            with io.BytesIO() as buffer:
                partition_df.drop(["year", "month"]).write_parquet(buffer)
                self.storage.upload_bytes(buffer.getvalue(), path)

        print(
            f"Final Fact Table: {df_events.height} listening events written to partitioned folders."
        )
        print("--- Silver Pipeline Complete ---")