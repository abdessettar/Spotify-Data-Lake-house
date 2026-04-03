import io
import polars as pl
from src.config import settings
from src.connectors.storage import DataLakeConnector

# TODO: this file is getting pretty long. Consider splitting into multiple files by domain area.
# TODO: convert utc to Brussels' time (current plan: conversion downstream when reading the tables).


class GoldPipeline:
    """Builds the Gold analytics layer from Silver tables."""

    def __init__(self):
        self.storage = DataLakeConnector()

    # ─── Silver readers ───────────────────────────────────────────────────────

    def _read_blob(self, path: str) -> pl.DataFrame:
        """Downloads a single-file Silver table. Returns empty DataFrame if missing."""
        try:
            cc = self.storage.blob_service.get_container_client(self.storage.container)
            raw = cc.download_blob(path).readall()
            return pl.read_parquet(io.BytesIO(raw))
        except Exception as e:
            print(f"  Warning: {path} not available ({type(e).__name__})")
            return pl.DataFrame()

    def _read_listening_events(self) -> pl.DataFrame:
        """Reads all Silver listening_events partitions via Polars scan_parquet."""
        path = f"az://{self.storage.container}/silver/listening_events/**/*.parquet"
        print("  Scanning silver/listening_events...")
        return pl.scan_parquet(
            path,
            storage_options=settings.polars_storage_options,
            hive_partitioning=True,
        ).collect()

    # ─── Gold builders ────────────────────────────────────────────────────────

    def _build_fact_plays(
        self,
        df_events: pl.DataFrame,
        df_tracks: pl.DataFrame,
        df_albums: pl.DataFrame,
        df_artists: pl.DataFrame,
        df_lastfm_artists: pl.DataFrame,
        df_af: pl.DataFrame,
    ) -> pl.DataFrame:
        """Joins all dimension data onto listening_events to produce the wide fact table."""
        print("  Building fact_plays...")

        # Step 1: rename duration_ms to clarify it's time *played*, add time dimensions.
        # dt.weekday() returns 1 (Monday) … 7 (Sunday).
        # TODO: add date dimension for better calendar based analysis
        # (e.g. holidays, seasons, etc.). Should be 1st step in a full data warehouse design.
        df = df_events.rename({"duration_ms": "duration_ms_played"}).with_columns(
            [
                pl.col("played_at_utc").dt.year().cast(pl.Int32).alias("played_year"),
                pl.col("played_at_utc").dt.month().cast(pl.Int32).alias("played_month"),
                pl.col("played_at_utc").dt.day().cast(pl.Int32).alias("played_day"),
                pl.col("played_at_utc").dt.hour().cast(pl.Int32).alias("played_hour"),
                pl.col("played_at_utc").dt.weekday().cast(pl.Int32).alias("played_dow"),
                (pl.col("played_at_utc").dt.weekday() >= 6).alias("is_weekend"),
            ]
        )

        # Step 2: join tracks → album_id, full track duration, explicit, track_popularity.
        if not df_tracks.is_empty():
            cols = ["track_id"]
            renames = {}
            for col, alias in [
                ("album_id", "album_id"),
                ("duration_ms", "duration_ms_track"),
                ("explicit", "explicit"),
                ("popularity", "track_popularity"),
                ("isrc", "isrc"),
            ]:
                if col in df_tracks.columns:
                    cols.append(col)
                    if alias != col:
                        renames[col] = alias
            df = df.join(
                df_tracks.select(cols).rename(renames), on="track_id", how="left"
            )

        # Step 3: join albums → album metadata.
        if not df_albums.is_empty() and "album_id" in df.columns:
            cols = ["album_id"]
            renames = {}
            for col, alias in [
                ("album_name", "_album_from_dim"),
                ("album_type", "album_type"),
                ("release_date", "release_date"),
                ("popularity", "album_popularity"),
                ("label", "album_label"),
            ]:
                if col in df_albums.columns:
                    cols.append(col)
                    if alias != col:
                        renames[col] = alias
            df = df.join(
                df_albums.select(cols).rename(renames), on="album_id", how="left"
            )

            # Coalesce album_name: the event column has it for extended_export records;
            # fall back to the albums dimension for all other sources.
            if "_album_from_dim" in df.columns:
                df = df.with_columns(
                    pl.coalesce(["album_name", "_album_from_dim"]).alias("album_name")
                ).drop("_album_from_dim")

            # release_year: slice first 4 chars of release_date (format varies).
            if "release_date" in df.columns:
                df = df.with_columns(
                    pl.col("release_date")
                    .str.slice(0, 4)
                    .cast(pl.Int32, strict=False)
                    .alias("release_year")
                )

        # Step 4: completion metrics: only meaningful for export sources where
        # duration_ms_played is actual time played (!= full track duration for API events).
        if "duration_ms_track" in df.columns:
            df = df.with_columns(
                [
                    pl.when(
                        pl.col("source_type").is_in(["extended_export", "export"])
                        & pl.col("duration_ms_track").is_not_null()
                        & (pl.col("duration_ms_track") > 0)
                    )
                    .then(
                        (
                            pl.col("duration_ms_played").cast(pl.Float64)
                            / pl.col("duration_ms_track").cast(pl.Float64)
                        ).round(4)
                    )
                    .otherwise(pl.lit(None).cast(pl.Float64))
                    .alias("completion_pct"),
                    # is_complete: Spotify counts a play as a stream after 30 seconds.
                    pl.when(pl.col("source_type").is_in(["extended_export", "export"]))
                    .then(pl.col("duration_ms_played") >= 30_000)
                    .otherwise(pl.lit(None).cast(pl.Boolean))
                    .alias("is_complete"),
                ]
            )

        # Step 5: join artists by artist_name to resolve artist_id.
        # Silver has no track → artist FK. the only artist signal on the fact is the
        # denormalized artist_name. A name-based join is the best we can do here.
        if not df_artists.is_empty() and "artist_name" in df_artists.columns:
            artist_cols = [
                c for c in ["artist_id", "artist_name"] if c in df_artists.columns
            ]
            df = df.join(df_artists.select(artist_cols), on="artist_name", how="left")

        # Step 6: join lastfm_artists via artist_id → top genre tag.
        if (
            not df_lastfm_artists.is_empty()
            and "artist_id" in df.columns
            and "top_tag" in df_lastfm_artists.columns
        ):
            df = df.join(
                df_lastfm_artists.select(
                    ["artist_id", pl.col("top_tag").alias("top_genre")]
                ),
                on="artist_id",
                how="left",
            )

        # Step 7: join audio features.
        if not df_af.is_empty():
            af_cols = [
                c
                for c in [
                    "track_id",
                    "danceability",
                    "energy",
                    "valence",
                    "tempo",
                    "acousticness",
                    "instrumentalness",
                    "speechiness",
                    "loudness",
                    "liveness",
                    "key",
                    "mode",
                    "time_signature",
                ]
                if c in df_af.columns
            ]
            df = df.join(df_af.select(af_cols), on="track_id", how="left")

        # Step 8: mood quadrant derived from energy × valence.
        if "energy" in df.columns and "valence" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("energy").is_null() | pl.col("valence").is_null())
                .then(pl.lit(None).cast(pl.Utf8))
                .when((pl.col("energy") > 0.5) & (pl.col("valence") > 0.5))
                .then(pl.lit("Euphoric"))
                .when((pl.col("energy") > 0.5) & (pl.col("valence") <= 0.5))
                .then(pl.lit("Intense"))
                .when((pl.col("energy") <= 0.5) & (pl.col("valence") > 0.5))
                .then(pl.lit("Chill"))
                .otherwise(pl.lit("Melancholic"))
                .alias("mood_quadrant")
            )

        return df

    def _build_dim_tracks(
        self,
        df_tracks: pl.DataFrame,
        df_albums: pl.DataFrame,
        df_lastfm: pl.DataFrame,
        df_af: pl.DataFrame,
    ) -> pl.DataFrame:
        """Wide track dimension: tracks + albums + Last.fm track tags + audio features."""
        print("  Building dim_tracks...")
        if df_tracks.is_empty():
            return pl.DataFrame()

        df = df_tracks.clone()

        # Join albums: prefix colliding columns to avoid ambiguity.
        if not df_albums.is_empty() and "album_id" in df.columns:
            renames = {}
            if "popularity" in df_albums.columns:
                renames["popularity"] = "album_popularity"
            if "label" in df_albums.columns:
                renames["label"] = "album_label"
            if "upc" in df_albums.columns:
                renames["upc"] = "album_upc"
            df = df.join(df_albums.rename(renames), on="album_id", how="left")

        if "release_date" in df.columns:
            df = df.with_columns(
                pl.col("release_date")
                .str.slice(0, 4)
                .cast(pl.Int32, strict=False)
                .alias("release_year")
            )

        # Join Last.fm track-level tags (prefix to avoid clash with artist-level lastfm).
        if not df_lastfm.is_empty():
            renames = {}
            for col, alias in [
                ("top_tag", "lastfm_top_tag"),
                ("tags", "lastfm_tags"),
                ("listeners", "lastfm_listeners"),
                ("playcount", "lastfm_playcount"),
            ]:
                if col in df_lastfm.columns:
                    renames[col] = alias
            df = df.join(df_lastfm.rename(renames), on="track_id", how="left")

        # Join audio features.
        if not df_af.is_empty():
            af_cols = [
                c
                for c in df_af.columns
                if c
                in [
                    "track_id",
                    "danceability",
                    "energy",
                    "valence",
                    "tempo",
                    "acousticness",
                    "instrumentalness",
                    "speechiness",
                    "loudness",
                    "liveness",
                    "key",
                    "mode",
                    "time_signature",
                ]
            ]
            df = df.join(df_af.select(af_cols), on="track_id", how="left")

        return df

    def _build_dim_artists(
        self,
        df_artists: pl.DataFrame,
        df_lastfm_artists: pl.DataFrame,
    ) -> pl.DataFrame:
        """Wide artist dimension: artists + Last.fm artist tags + derived genre columns."""
        print("  Building dim_artists...")
        if df_artists.is_empty():
            return pl.DataFrame()

        df = df_artists.clone()

        # primary_genre: first entry of Spotify's official genres list.
        if "genres" in df.columns:
            df = df.with_columns(pl.col("genres").list.first().alias("primary_genre"))

        # Join Last.fm artist tags.
        if not df_lastfm_artists.is_empty():
            renames = {}
            if "top_tag" in df_lastfm_artists.columns:
                renames["top_tag"] = "lastfm_top_tag"
            if "tags" in df_lastfm_artists.columns:
                renames["tags"] = "lastfm_tags"
            df = df.join(df_lastfm_artists.rename(renames), on="artist_id", how="left")

        # combined_genres: union of Spotify genres + Last.fm tags, deduplicated.
        # fill_null([]) converts nulls to empty lists so concat_list never fails;
        # then we replace empty results back with null.
        has_genres = "genres" in df.columns
        has_lastfm = "lastfm_tags" in df.columns
        if has_genres and has_lastfm:
            df = df.with_columns(
                pl.concat_list(
                    [
                        pl.col("genres").fill_null([]),
                        pl.col("lastfm_tags").fill_null([]),
                    ]
                )
                .list.unique()
                .alias("combined_genres")
            ).with_columns(
                pl.when(pl.col("combined_genres").list.len() == 0)
                .then(pl.lit(None).cast(pl.List(pl.Utf8)))
                .otherwise(pl.col("combined_genres"))
                .alias("combined_genres")
            )
        elif has_genres:
            df = df.with_columns(pl.col("genres").alias("combined_genres"))
        elif has_lastfm:
            df = df.with_columns(pl.col("lastfm_tags").alias("combined_genres"))

        return df

    def _build_agg_daily(self, df_fact: pl.DataFrame) -> pl.DataFrame:
        """One row per calendar day: play counts, listening time, mood averages, top artist/genre."""
        print("  Building agg_daily...")
        if df_fact.is_empty():
            return pl.DataFrame()

        df = df_fact.with_columns(pl.col("played_at_utc").dt.date().alias("date"))

        agg_exprs = [
            pl.len().alias("total_plays"),
            pl.col("duration_ms_played").sum().alias("total_ms_listened"),
            pl.col("track_id").n_unique().alias("unique_tracks"),
            pl.col("artist_name").n_unique().alias("unique_artists"),
        ]
        for col, alias in [
            ("is_complete", "complete_plays"),
            ("skipped", "skip_count"),
        ]:
            if col in df.columns:
                agg_exprs.append(pl.col(col).cast(pl.Int64).sum().alias(alias))
        for col, alias in [
            ("energy", "avg_energy"),
            ("valence", "avg_valence"),
            ("danceability", "avg_danceability"),
        ]:
            if col in df.columns:
                agg_exprs.append(pl.col(col).mean().alias(alias))

        df_daily = df.group_by("date").agg(agg_exprs).sort("date")

        # Top artist per day: sort by play count desc within day, take first.
        top_artist = (
            df.group_by(["date", "artist_name"])
            .agg(pl.len().alias("n"))
            .sort(["date", "n"], descending=[False, True])
            .unique(subset=["date"], keep="first")
            .select(["date", pl.col("artist_name").alias("top_artist")])
        )
        df_daily = df_daily.join(top_artist, on="date", how="left")

        if "top_genre" in df.columns:
            top_genre = (
                df.filter(pl.col("top_genre").is_not_null())
                .group_by(["date", "top_genre"])
                .agg(pl.len().alias("n"))
                .sort(["date", "n"], descending=[False, True])
                .unique(subset=["date"], keep="first")
                .select(["date", "top_genre"])
            )
            df_daily = df_daily.join(top_genre, on="date", how="left")

        return df_daily

    def _build_agg_monthly(self, df_fact: pl.DataFrame) -> pl.DataFrame:
        """One row per (year, month): play stats, mood averages, new artist discovery count."""
        print("  Building agg_monthly...")
        if df_fact.is_empty():
            return pl.DataFrame()

        # Use prefixed columns to avoid clashing with played_year/played_month in fact.
        df = df_fact.with_columns(
            [
                pl.col("played_at_utc").dt.year().cast(pl.Int32).alias("_yr"),
                pl.col("played_at_utc").dt.month().cast(pl.Int32).alias("_mo"),
            ]
        )

        agg_exprs = [
            pl.len().alias("total_plays"),
            pl.col("duration_ms_played").sum().alias("total_ms_listened"),
            pl.col("track_id").n_unique().alias("unique_tracks"),
            pl.col("artist_name").n_unique().alias("unique_artists"),
        ]
        for col, alias in [
            ("is_complete", "complete_plays"),
            ("skipped", "skip_count"),
        ]:
            if col in df_fact.columns:
                agg_exprs.append(pl.col(col).cast(pl.Int64).sum().alias(alias))
        for col, alias in [
            ("energy", "avg_energy"),
            ("valence", "avg_valence"),
            ("danceability", "avg_danceability"),
        ]:
            if col in df_fact.columns:
                agg_exprs.append(pl.col(col).mean().alias(alias))

        df_monthly = (
            df.group_by(["_yr", "_mo"])
            .agg(agg_exprs)
            .rename({"_yr": "year", "_mo": "month"})
            .sort(["year", "month"])
        )

        # new_artists: artists heard for the very first time this calendar month.
        new_artists = (
            df_fact.sort("played_at_utc")
            .group_by("artist_name")
            .agg(pl.col("played_at_utc").first().alias("first_at"))
            .with_columns(
                [
                    pl.col("first_at").dt.year().cast(pl.Int32).alias("year"),
                    pl.col("first_at").dt.month().cast(pl.Int32).alias("month"),
                ]
            )
            .group_by(["year", "month"])
            .agg(pl.len().alias("new_artists"))
        )
        df_monthly = df_monthly.join(new_artists, on=["year", "month"], how="left")

        # Top artist per month:
        top_artist = (
            df.group_by(["_yr", "_mo", "artist_name"])
            .agg(pl.len().alias("n"))
            .sort(["_yr", "_mo", "n"], descending=[False, False, True])
            .unique(subset=["_yr", "_mo"], keep="first")
            .rename({"_yr": "year", "_mo": "month"})
            .select(["year", "month", pl.col("artist_name").alias("top_artist")])
        )
        df_monthly = df_monthly.join(top_artist, on=["year", "month"], how="left")

        return df_monthly

    def _build_agg_artist_stats(
        self, df_fact: pl.DataFrame, df_dim_artists: pl.DataFrame
    ) -> pl.DataFrame:
        """Per-artist lifetime stats: plays, listening time, skip rate, mood averages."""
        print("  Building agg_artist_stats...")
        if df_fact.is_empty():
            return pl.DataFrame()

        agg_exprs = [
            pl.len().alias("total_plays"),
            pl.col("duration_ms_played").sum().alias("total_ms_listened"),
            pl.col("track_id").n_unique().alias("unique_tracks"),
            pl.col("played_at_utc").min().alias("first_played_at"),
            pl.col("played_at_utc").max().alias("last_played_at"),
        ]
        for col, alias in [
            ("energy", "avg_energy"),
            ("valence", "avg_valence"),
            ("danceability", "avg_danceability"),
        ]:
            if col in df_fact.columns:
                agg_exprs.append(pl.col(col).mean().alias(alias))
        if "skipped" in df_fact.columns:
            agg_exprs.extend(
                [
                    pl.col("skipped").cast(pl.Int64).sum().alias("skip_count"),
                    (pl.col("skipped").cast(pl.Float64).sum() / pl.len()).alias(
                        "skip_rate"
                    ),
                ]
            )

        df_stats = df_fact.group_by("artist_name").agg(agg_exprs)

        # Enrich with dim_artists metadata (artist_id, primary_genre, followers, etc.).
        if not df_dim_artists.is_empty():
            dim_cols = [
                c
                for c in [
                    "artist_name",
                    "artist_id",
                    "primary_genre",
                    "followers_total",
                    "popularity",
                    "lastfm_top_tag",
                ]
                if c in df_dim_artists.columns
            ]
            df_stats = df_stats.join(
                df_dim_artists.select(dim_cols), on="artist_name", how="left"
            )

        return df_stats.sort("total_plays", descending=True)

    def _build_agg_track_stats(self, df_fact: pl.DataFrame) -> pl.DataFrame:
        """Per-track lifetime stats: plays, listening time, skip rate, first/last heard."""
        print("  Building agg_track_stats...")
        if df_fact.is_empty():
            return pl.DataFrame()

        # Only tracks with a resolved track_id (excludes pre-enrichment export events).
        df = df_fact.filter(pl.col("track_id").is_not_null())
        if df.is_empty():
            return pl.DataFrame()

        agg_exprs = [
            pl.col("track_name").first().alias("track_name"),
            pl.col("artist_name").first().alias("artist_name"),
            pl.len().alias("total_plays"),
            pl.col("duration_ms_played").sum().alias("total_ms_listened"),
            pl.col("played_at_utc").min().alias("first_played_at"),
            pl.col("played_at_utc").max().alias("last_played_at"),
        ]
        for col in ["album_name", "energy", "valence", "danceability"]:
            if col in df.columns:
                agg_exprs.append(pl.col(col).first().alias(col))
        if "track_popularity" in df.columns:
            agg_exprs.append(pl.col("track_popularity").first().alias("popularity"))
        if "skipped" in df.columns:
            agg_exprs.extend(
                [
                    pl.col("skipped").cast(pl.Int64).sum().alias("skip_count"),
                    (pl.col("skipped").cast(pl.Float64).sum() / pl.len()).alias(
                        "skip_rate"
                    ),
                ]
            )

        return (
            df.group_by("track_id").agg(agg_exprs).sort("total_plays", descending=True)
        )

    # ─── Writers ──────────────────────────────────────────────────────────────

    def _write_table(self, df: pl.DataFrame, path: str):
        """Uploads a single-file Gold table to Azure Blob Storage."""
        with io.BytesIO() as buf:
            df.write_parquet(buf)
            self.storage.upload_bytes(buf.getvalue(), path)

    def _write_partitioned(self, df: pl.DataFrame, base_path: str):
        """Writes fact_plays with Hive partitioning on played_year/played_month."""
        for (year, month), part in df.group_by(["played_year", "played_month"]):
            path = f"{base_path}/year={year}/month={month}/data.parquet"
            with io.BytesIO() as buf:
                part.write_parquet(buf)
                self.storage.upload_bytes(buf.getvalue(), path)

    # ─── Entry point ──────────────────────────────────────────────────────────

    def run(self):
        print("--- Starting Gold Analytics Pipeline ---")

        print("Loading Silver tables...")
        df_events = self._read_listening_events()
        df_tracks = self._read_blob("silver/tracks/data.parquet")
        df_albums = self._read_blob("silver/albums/data.parquet")
        df_artists = self._read_blob("silver/artists/data.parquet")
        df_lastfm = self._read_blob("silver/lastfm/data.parquet")
        df_lastfm_artists = self._read_blob("silver/lastfm_artists/data.parquet")
        df_af = self._read_blob("silver/audio_features/data.parquet")

        print(f"  listening_events  : {df_events.height:,} rows")
        print(f"  tracks            : {df_tracks.height:,} rows")
        print(f"  artists           : {df_artists.height:,} rows")
        print(f"  albums            : {df_albums.height:,} rows")
        print(f"  audio_features    : {df_af.height:,} rows")

        print("Building Gold tables...")
        df_dim_tracks = self._build_dim_tracks(df_tracks, df_albums, df_lastfm, df_af)
        df_dim_artists = self._build_dim_artists(df_artists, df_lastfm_artists)
        df_fact = self._build_fact_plays(
            df_events, df_tracks, df_albums, df_artists, df_lastfm_artists, df_af
        )
        df_daily = self._build_agg_daily(df_fact)
        df_monthly = self._build_agg_monthly(df_fact)
        df_artist_stats = self._build_agg_artist_stats(df_fact, df_dim_artists)
        df_track_stats = self._build_agg_track_stats(df_fact)

        print("Writing Gold tables to Azure...")
        if not df_dim_tracks.is_empty():
            self._write_table(df_dim_tracks, "gold/dim_tracks/data.parquet")
            print(f"  dim_tracks        : {df_dim_tracks.height:,} rows")
        if not df_dim_artists.is_empty():
            self._write_table(df_dim_artists, "gold/dim_artists/data.parquet")
            print(f"  dim_artists       : {df_dim_artists.height:,} rows")
        if not df_fact.is_empty():
            self._write_partitioned(df_fact, "gold/fact_plays")
            print(
                f"  fact_plays        : {df_fact.height:,} rows (partitioned by year/month)"
            )
        if not df_daily.is_empty():
            self._write_table(df_daily, "gold/agg_daily/data.parquet")
            print(f"  agg_daily         : {df_daily.height:,} rows")
        if not df_monthly.is_empty():
            self._write_table(df_monthly, "gold/agg_monthly/data.parquet")
            print(f"  agg_monthly       : {df_monthly.height:,} rows")
        if not df_artist_stats.is_empty():
            self._write_table(df_artist_stats, "gold/agg_artist_stats/data.parquet")
            print(f"  agg_artist_stats  : {df_artist_stats.height:,} rows")
        if not df_track_stats.is_empty():
            self._write_table(df_track_stats, "gold/agg_track_stats/data.parquet")
            print(f"  agg_track_stats   : {df_track_stats.height:,} rows")

        print("--- Gold Pipeline Complete ---")
