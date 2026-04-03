import io
import json
import base64
import polars as pl

from src.config import settings
from src.connectors.storage import DataLakeConnector

# git ignored and used locally only: very large and were torrented (not very legal...).
DUMPS_DIR = "data/dumps"


class DumpEnrichmentPipeline:
    def __init__(self):
        self.storage = DataLakeConnector()
        self.storage_options = settings.polars_storage_options

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _list_silver_files(self, prefix: str) -> list[str]:
        """Lists Silver Parquet files under a given prefix via the Azure SDK."""
        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )
        return [
            f"az://{self.storage.container}/{blob.name}"
            for blob in container_client.list_blobs(name_starts_with=prefix)
            if blob.name.endswith(".parquet")
        ]

    def _read_silver_table(self, blob_path: str) -> pl.DataFrame:
        """Downloads and deserialises a single-file Silver dimension table."""
        try:
            raw = (
                self.storage.blob_service.get_container_client(self.storage.container)
                .download_blob(blob_path)
                .readall()
            )
            return pl.read_parquet(io.BytesIO(raw))
        except Exception:
            return pl.DataFrame()

    def _write_silver_table(self, df: pl.DataFrame, blob_path: str):
        """Serialises a DataFrame to Parquet and uploads it to Silver."""
        with io.BytesIO() as buffer:
            df.write_parquet(buffer)
            self.storage.upload_bytes(buffer.getvalue(), blob_path)

    # -------------------------------------------------------------------------
    # Phase 1 — Track ID resolution
    # -------------------------------------------------------------------------

    def _phase1_resolve_track_ids(self):
        """
        Matches export events with null track_id against the dump by (track_name,
        artist_name) and writes Bronze spotify_enrichment markers so that the next
        `transform` run can fill in the missing IDs on the listening_events fact table.
        """
        print("\n--- Phase 1: Resolving missing track IDs from dump ---")

        event_files = self._list_silver_files("silver/listening_events/")
        if not event_files:
            print("No Silver listening_events found. Run transform first.")
            return

        # Collect distinct (track_name, artist_name) pairs that still lack a track_id.
        df_missing = (
            pl.scan_parquet(
                event_files,
                storage_options=self.storage_options,
                hive_partitioning=True,
            )
            .filter(pl.col("track_id").is_null())
            .select(["track_name", "artist_name"])
            .unique()
            .collect()
            .drop_nulls()
        )

        if df_missing.is_empty():
            print("No events with missing track_id. Skipping.")
            return

        print(
            f"Found {df_missing.height} (track_name, artist_name) pairs with no track_id."
        )

        # Skip pairs already attempted (files exist in Bronze enrichment).
        existing_files = self.storage.list_bronze_files("spotify_enrichment")
        already_attempted: set[tuple[str, str]] = set()
        for f in existing_files:
            filename = f.split("/")[-1].replace(".json", "")
            try:
                decoded = base64.b64decode(filename).decode("utf-8")
                # Bronze markers written by this pipeline use "artist||track" keys.
                if "||" in decoded:
                    parts = decoded.split("||", 1)
                    already_attempted.add((parts[0], parts[1]))
            except Exception:
                pass

        pairs_to_resolve = [
            (row["track_name"], row["artist_name"])
            for row in df_missing.iter_rows(named=True)
            if (row["artist_name"], row["track_name"]) not in already_attempted
        ]

        if not pairs_to_resolve:
            print("All missing pairs already attempted. Skipping.")
            return

        print(
            f"Looking up {len(pairs_to_resolve)} pairs in dump (this may take a moment)..."
        )

        # ------------------------------------------------------------------
        # Build a joined lookup table from the dump:
        #   tracks → track_artists → artists
        # Filtered by track names in our pairs to avoid loading all 256M rows.
        # ------------------------------------------------------------------
        track_names_needed = {p[0] for p in pairs_to_resolve}

        df_dump_tracks = (
            pl.scan_parquet(f"{DUMPS_DIR}/tracks.parquet")
            .filter(pl.col("name").is_in(track_names_needed))
            .select(
                [
                    "rowid",
                    "id",
                    "name",
                    "duration_ms",
                    "explicit",
                    "popularity",
                    "track_number",
                    "disc_number",
                    "external_id_isrc",
                    "album_rowid",
                ]
            )
            .collect()
        )

        if df_dump_tracks.is_empty():
            print("No track name matches found in dump.")
            return

        # Load albums for matched tracks.
        album_rowids = df_dump_tracks["album_rowid"].unique().to_list()
        df_dump_albums = (
            pl.scan_parquet(f"{DUMPS_DIR}/albums.parquet")
            .filter(pl.col("rowid").is_in(album_rowids))
            .select(
                ["rowid", "id", "name", "album_type", "release_date", "total_tracks"]
            )
            .collect()
            .rename({"rowid": "album_rowid", "id": "album_id", "name": "album_name"})
        )

        # Load artists for matched tracks (via track_artists junction).
        track_rowids = df_dump_tracks["rowid"].to_list()
        df_track_artists = (
            pl.scan_parquet(f"{DUMPS_DIR}/track_artists.parquet")
            .filter(pl.col("track_rowid").is_in(track_rowids))
            .collect()
        )

        artist_rowids = df_track_artists["artist_rowid"].unique().to_list()
        df_dump_artists = (
            pl.scan_parquet(f"{DUMPS_DIR}/artists.parquet")
            .filter(pl.col("rowid").is_in(artist_rowids))
            .select(["rowid", "id", "name"])
            .collect()
            .rename(
                {"rowid": "artist_rowid", "id": "artist_id", "name": "artist_name_dump"}
            )
        )

        # Join track_artists → artists, then group artists by track.
        df_artists_by_track = (
            df_track_artists.join(df_dump_artists, on="artist_rowid", how="left")
            .group_by("track_rowid")
            .agg(
                [
                    pl.col("artist_id").alias("artist_ids"),
                    pl.col("artist_name_dump").alias("artist_names"),
                ]
            )
        )

        # Full join: tracks → albums → artists.
        df_full = df_dump_tracks.join(
            df_dump_albums, on="album_rowid", how="left"
        ).join(df_artists_by_track, left_on="rowid", right_on="track_rowid", how="left")

        # ------------------------------------------------------------------
        # Write a Bronze marker for each resolved pair.
        # ------------------------------------------------------------------
        container_client = self.storage.blob_service.get_container_client(
            self.storage.container
        )
        resolved = 0
        not_found = 0

        for track_name, artist_name in pairs_to_resolve:
            matches = df_full.filter(
                (pl.col("name") == track_name)
                & (pl.col("artist_names").list.contains(artist_name))
            )

            key = f"{artist_name}||{track_name}"
            safe_name = base64.b64encode(key.encode()).decode()
            blob_path = f"bronze/spotify_enrichment/{safe_name}.json"

            if matches.is_empty():
                container_client.upload_blob(
                    name=blob_path, data=json.dumps({"not_found": True}), overwrite=True
                )
                not_found += 1
                continue

            row = matches.row(0, named=True)
            artist_ids = row.get("artist_ids") or []
            artist_names_list = row.get("artist_names") or []

            # Reconstruct a nested track object compatible with _extract_dimensions().
            # images is a list with one dummy struct so Polars infers the struct schema
            # correctly when mixing these markers with API-fetched ones.
            artists = [
                {"id": aid, "name": aname, "uri": f"spotify:artist:{aid}"}
                for aid, aname in zip(artist_ids, artist_names_list)
                if aid
            ]

            record = {
                "search_artist_name": artist_name,
                "search_track_name": track_name,
                "track": {
                    "id": row["id"],
                    "name": row["name"],
                    "duration_ms": row["duration_ms"],
                    "explicit": bool(row["explicit"])
                    if row["explicit"] is not None
                    else False,
                    "popularity": row["popularity"],
                    "track_number": row["track_number"],
                    "disc_number": row["disc_number"],
                    "uri": f"spotify:track:{row['id']}",
                    "external_ids": {"isrc": row["external_id_isrc"]},
                    "artists": artists,
                    "album": {
                        "id": row.get("album_id"),
                        "name": row.get("album_name"),
                        "album_type": row.get("album_type"),
                        "release_date": row.get("release_date"),
                        "total_tracks": row.get("total_tracks"),
                        "images": [{"url": None, "height": None, "width": None}],
                    },
                },
            }

            container_client.upload_blob(
                name=blob_path, data=json.dumps(record, indent=2), overwrite=True
            )
            resolved += 1

        print(
            f"Phase 1 complete: {resolved} resolved, {not_found} not found in dump. "
            "Re-run 'transform' to apply resolved IDs to listening_events."
        )

    # -------------------------------------------------------------------------
    # Phase 2 — Track detail fill
    # -------------------------------------------------------------------------

    def _phase2_enrich_tracks(self):
        """
        Fills isrc (where still null), popularity, and disc_number on the Silver
        tracks dimension by joining against the dump tracks table.
        """
        print("\n--- Phase 2: Enriching Silver tracks from dump ---")

        df_silver = self._read_silver_table("silver/tracks/data.parquet")
        if df_silver.is_empty():
            print("No Silver tracks table found. Run transform first.")
            return

        track_ids = df_silver["track_id"].drop_nulls().unique().to_list()
        print(f"Looking up {len(track_ids)} track IDs in dump...")

        df_dump = (
            pl.scan_parquet(f"{DUMPS_DIR}/tracks.parquet")
            .filter(pl.col("id").is_in(track_ids))
            .select(["id", "external_id_isrc", "popularity", "disc_number"])
            .collect()
            .rename(
                {
                    "id": "track_id",
                    "external_id_isrc": "_isrc_dump",
                    "popularity": "_popularity_dump",
                    "disc_number": "_disc_number_dump",
                }
            )
        )

        print(f"Found {df_dump.height} matches in dump.")

        df_enriched = df_silver.join(df_dump, on="track_id", how="left")

        # Coalesce isrc: keep existing Silver value if present, fill from dump otherwise.
        if "isrc" in df_enriched.columns:
            df_enriched = df_enriched.with_columns(
                pl.coalesce(["isrc", "_isrc_dump"]).alias("isrc")
            )
        else:
            df_enriched = df_enriched.with_columns(pl.col("_isrc_dump").alias("isrc"))

        # Coalesce popularity: same logic.
        if "popularity" in df_enriched.columns:
            df_enriched = df_enriched.with_columns(
                pl.coalesce(
                    [
                        pl.col("popularity").cast(pl.Int64),
                        pl.col("_popularity_dump").cast(pl.Int64),
                    ]
                ).alias("popularity")
            )
        else:
            df_enriched = df_enriched.with_columns(
                pl.col("_popularity_dump").cast(pl.Int64).alias("popularity")
            )

        # disc_number is new: add it directly.
        df_enriched = df_enriched.with_columns(
            pl.col("_disc_number_dump").cast(pl.Int64).alias("disc_number")
        ).drop(["_isrc_dump", "_popularity_dump", "_disc_number_dump"])

        self._write_silver_table(df_enriched, "silver/tracks/data.parquet")
        print(f"Phase 2 complete: {df_dump.height} tracks enriched.")

    # -------------------------------------------------------------------------
    # Phase 3 — Artist genre and metadata enrichment
    # -------------------------------------------------------------------------

    def _phase3_enrich_artists(self):
        """
        Adds followers_total, popularity, and genres (List[String]) to the Silver
        artists dimension. Genres come from the dump's artist_genres junction table
        and use Spotify's official taxonomy, replacing Last.fm crowd-sourced tags
        as the canonical source of artist genre data (last.fm was the previous source).
        """
        print("\n--- Phase 3: Enriching Silver artists from dump ---")

        df_silver = self._read_silver_table("silver/artists/data.parquet")
        if df_silver.is_empty():
            print("No Silver artists table found. Run transform first.")
            return

        artist_ids = df_silver["artist_id"].drop_nulls().unique().to_list()
        print(f"Looking up {len(artist_ids)} artist IDs in dump...")

        df_dump_artists = (
            pl.scan_parquet(f"{DUMPS_DIR}/artists.parquet")
            .filter(pl.col("id").is_in(artist_ids))
            .select(["rowid", "id", "followers_total", "popularity"])
            .collect()
        )

        print(f"Found {df_dump_artists.height} matches in dump. Loading genres...")

        artist_rowids = df_dump_artists["rowid"].unique().to_list()
        df_genres = (
            pl.scan_parquet(f"{DUMPS_DIR}/artist_genres.parquet")
            .filter(pl.col("artist_rowid").is_in(artist_rowids))
            .collect()
            .group_by("artist_rowid")
            .agg(pl.col("genre").alias("genres"))
        )

        # Join genres onto artists, then join to Silver.
        df_dump_enriched = (
            df_dump_artists.join(
                df_genres, left_on="rowid", right_on="artist_rowid", how="left"
            )
            .rename(
                {
                    "id": "artist_id",
                    "followers_total": "_followers_dump",
                    "popularity": "_popularity_dump",
                }
            )
            .drop("rowid")
        )

        df_enriched = (
            df_silver.join(df_dump_enriched, on="artist_id", how="left")
            .with_columns(
                [
                    pl.col("_followers_dump").cast(pl.Int64).alias("followers_total"),
                    pl.col("_popularity_dump").cast(pl.Int64).alias("popularity"),
                    pl.col("genres").cast(pl.List(pl.Utf8)),
                ]
            )
            .drop(["_followers_dump", "_popularity_dump"])
        )

        self._write_silver_table(df_enriched, "silver/artists/data.parquet")
        print(
            f"Phase 3 complete: {df_dump_artists.height} artists enriched with metadata and genres."
        )

    # -------------------------------------------------------------------------
    # Phase 4 — Album detail fill
    # -------------------------------------------------------------------------

    def _phase4_enrich_albums(self):
        """
        Adds label, upc, and popularity to the Silver albums dimension by joining
        against the dump albums table.
        """
        print("\n--- Phase 4: Enriching Silver albums from dump ---")

        df_silver = self._read_silver_table("silver/albums/data.parquet")
        if df_silver.is_empty():
            print("No Silver albums table found. Run transform first.")
            return

        album_ids = df_silver["album_id"].drop_nulls().unique().to_list()
        print(f"Looking up {len(album_ids)} album IDs in dump...")

        df_dump = (
            pl.scan_parquet(f"{DUMPS_DIR}/albums.parquet")
            .filter(pl.col("id").is_in(album_ids))
            .select(["id", "label", "external_id_upc", "popularity"])
            .collect()
            .rename(
                {
                    "id": "album_id",
                    "external_id_upc": "upc",
                    "popularity": "_popularity_dump",
                }
            )
        )

        print(f"Found {df_dump.height} matches in dump.")

        df_enriched = (
            df_silver.join(df_dump, on="album_id", how="left")
            .with_columns(pl.col("_popularity_dump").cast(pl.Int64).alias("popularity"))
            .drop("_popularity_dump")
        )

        self._write_silver_table(df_enriched, "silver/albums/data.parquet")
        print(f"Phase 4 complete: {df_dump.height} albums enriched.")

    # -------------------------------------------------------------------------
    # Phase 5 — Audio features
    # -------------------------------------------------------------------------

    def _phase5_audio_features(self):
        """
        Builds the silver/audio_features/data.parquet table from the deprecated
        Spotify audio features dump (255M rows). Filters to our Silver track IDs,
        excludes null-response rows, and casts all feature columns from string to
        their proper numeric types.
        """
        print("\n--- Phase 5: Building audio_features Silver table from dump ---")

        track_files = self._list_silver_files("silver/tracks/")
        if not track_files:
            print("No Silver tracks table found. Run transform first.")
            return

        track_ids = (
            pl.scan_parquet(track_files, storage_options=self.storage_options)
            .select("track_id")
            .collect()["track_id"]
            .drop_nulls()
            .unique()
            .to_list()
        )

        print(f"Loading audio features for {len(track_ids)} tracks from dump...")

        df_features = (
            pl.scan_parquet(f"{DUMPS_DIR}/track_audio_features.parquet")
            .filter(
                pl.col("track_id").is_in(track_ids) & (pl.col("null_response") == "0")
            )
            .select(
                [
                    "track_id",
                    "time_signature",
                    "tempo",
                    "key",
                    "mode",
                    "danceability",
                    "energy",
                    "loudness",
                    "speechiness",
                    "acousticness",
                    "instrumentalness",
                    "liveness",
                    "valence",
                ]
            )
            .collect()
        )

        if df_features.is_empty():
            print("No audio features found for our track IDs.")
            return

        # All columns are stored as strings in the dump -> cast to proper types.
        df_features = df_features.with_columns(
            [
                pl.col("time_signature").cast(pl.Int64),
                pl.col("key").cast(pl.Int64),
                pl.col("mode").cast(pl.Int64),
                pl.col("tempo").cast(pl.Float64),
                pl.col("danceability").cast(pl.Float64),
                pl.col("energy").cast(pl.Float64),
                pl.col("loudness").cast(pl.Float64),
                pl.col("speechiness").cast(pl.Float64),
                pl.col("acousticness").cast(pl.Float64),
                pl.col("instrumentalness").cast(pl.Float64),
                pl.col("liveness").cast(pl.Float64),
                pl.col("valence").cast(pl.Float64),
            ]
        )

        self._write_silver_table(df_features, "silver/audio_features/data.parquet")
        print(
            f"Phase 5 complete: {df_features.height} audio feature rows written to silver/audio_features/data.parquet."
        )

    # -------------------------------------------------------------------------
    # Entry point
    # -------------------------------------------------------------------------

    def run(self):
        print("=== Starting Dump Enrichment Pipeline ===")
        self._phase1_resolve_track_ids()
        self._phase2_enrich_tracks()
        self._phase3_enrich_artists()
        self._phase4_enrich_albums()
        self._phase5_audio_features()
        print("\n=== Dump Enrichment Pipeline Complete ===")
        print(
            "Re-run 'transform' to apply Phase 1 track ID resolutions to listening_events."
        )
