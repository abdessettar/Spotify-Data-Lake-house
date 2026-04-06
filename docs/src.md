# Source Code Documentation

The application code lives under `src/`, structured as a Python package with a CLI entry point, domain models, connectors, and pipelines.

```
src/
├── main.py              # CLI entry point (Typer)
├── config.py            # Settings & environment configuration
├── domain/
│   └── schemas.py       # Pydantic data models
├── core/
│   └── state.py         # Pipeline cursor / state management
├── connectors/
│   ├── spotify.py       # Spotify API client
│   ├── lastfm.py        # Last.fm API client
│   └── storage.py       # Azure Blob Storage I/O
└── pipelines/
    ├── ingestion.py     # Fetch recent tracks from Spotify API
    ├── backfill.py      # Upload historical Spotify exports
    ├── silver.py        # Bronze → Silver star schema transform
    ├── enrichment.py    # Resolve missing track IDs via Spotify search
    ├── lastfm_enrichment.py  # Fetch Last.fm tags & popularity
    ├── dump_enrichment.py    # Enrich from local Spotify catalogue dumps
    └── gold.py          # Silver → Gold analytics layer
```

Some scripts (`verify_*.py`) were used during development to verify some major feature added, they were not removed and are kept here as they can still be useful.

---

## Configuration (`config.py`)

The `Settings` class uses Pydantic to load configuration from `.env` or environment variables. It supports three modes:

| Mode | Auth Method | Use Case |
|---|---|---|
| **DEV** | Azurite connection string | Local development with storage emulator |
| **LOCAL** | Azure CLI credentials | Development against real Azure resources |
| **PROD** | Managed Identity | Container Apps Jobs in Azure |

In PROD mode, Spotify credentials are fetched from Azure Key Vault at startup, and the class also provides computed properties for fsspec and Polars storage options so downstream code does not need to know about authentication details.

---

## Entry Point (`main.py`)

A Typer CLI application that exposes these commands:

| Command | Description |
|---|---|
| `ingest` | Fetch recent tracks from Spotify API, save to Bronze |
| `backfill` | Upload historical Spotify export files to Bronze |
| `transform` | Convert Bronze JSON to Silver Parquet (star schema) |
| `enrich` | Search Spotify API to find missing track IDs (slow, local only) |
| `enrich-lastfm` | Fetch Last.fm tags and popularity for tracks and artists |
| `enrich-dumps` | Enrich using local Spotify catalogue dumps (5 phases) |
| `gold` | Build analytics layer from Silver tables |
| `transform-gold` | Run transform then gold (used by scheduled Container Apps Jobs) |
| `run-all` | Run ingest then transform (used by scheduled Container Apps Jobs) |

The two composite commands (`transform-gold`, `run-all`) exist because Container Apps Jobs each run a single command.

---

## Domain Models (`domain/schemas.py`)

Pydantic models defining the data contracts used throughout the codebase.

**Pipeline state:**
- `PipelineCursor`: Tracks `last_run_timestamp` and `last_played_at_unix_ms` for API pagination.

**Spotify API models:**
- `SpotifyImage`, `SpotifyArtist`, `SpotifyAlbum`, `SpotifyTrack`, `SpotifyContext`, `PlayedItem`, `RecentlyPlayedResponse` mirrors the Spotify API response structure.

**Spotify export models:**
- `SpotifyExportRecord`: Handles both standard and extended export formats using Pydantic `AliasChoices`. Standard exports use fields like `endTime` and `artistName`, while extended exports use `ts` and `master_metadata_album_artist_name`. An `extracted_track_id` property parses Spotify URIs (`spotify:track:XXXXX`).

**Last.fm models:**
- `LastfmTrackInfo`: Track metadata including tags, listeners, and playcount.

---

## State Management (`core/state.py`)

`StateManager` persists a pipeline cursor to Azure Blob Storage at `system/state/cursor.json` where it stores the timestamp of the last successfully ingested track so that subsequent runs only fetch new data from the Spotify API.

The default cursor starts at 1st of January 2017 (the account was created during that year), ensuring a fresh deployment captures the full history.

---

## Connectors

### Spotify Client (`connectors/spotify.py`)

Handles all Spotify API communication:
- **Token management**: Refreshes the OAuth access token using the refresh token with HTTP Basic auth. Automatically handles 401 responses by re-authenticating mid-request.
- `get_recently_played(after_timestamp_unix_ms)`: Fetches paginated recently-played tracks and retries 3 times with exponential backoff (2-10s) via Tenacity.
- `get_track_by_id(track_id)`: Fetches single track metadata from `/v1/tracks/{id}`.
- `search_track(track_name, artist_name)`: Searches Spotify using structured query syntax for enrichment.

### Last.fm Client (`connectors/lastfm.py`)

Fetches metadata from the Last.fm API:
- `get_track_info(track_name, artist_name)`: Returns tags, listeners, and playcount using `autocorrect=1` for spelling normalization and filters out noise tags (e.g. "seen live", "favorite", "spotify") defined in a `NOISE_TAGS` set.
- `get_artist_tags(artist_name)`: Fetches top tags for an artist, which tend to be better populated than track-level tags.

Handles Last.fm's inconsistent JSON format where tags can be either a single item or a list.

### Storage Connector (`connectors/storage.py`)

`DataLakeConnector` manages all Blob Storage I/O:
- `save_raw_played_items()`: Saves API responses to Bronze with Hive partitioning (`year=/month=/day=`).
- `save_backfill_file()`: Uploads export JSON to `bronze/spotify_export/`.
- `list_bronze_files(source)`: Lists files by source (spotify_api, spotify_export, etc.).
- `upload_bytes()`: Generic blob upload with size logging.

---

## Pipelines

### 1. Ingestion (`pipelines/ingestion.py`)

Fetches recent tracks from the Spotify API and writes them to Bronze:
1. Reads the current cursor from state manager.
2. Calls the Spotify API with `after_timestamp_unix_ms` for pagination.
3. Saves raw `PlayedItem` JSON to Bronze with Hive-partitioned paths.
4. Updates the cursor to the most recent track's `played_at` time.

### 2. Backfill (`pipelines/backfill.py`)

Uploads historical Spotify data exports (requested frolm Spotify and take few days to be available):
1. Finds JSON files matching `StreamingHistory` or `Streaming_History` filename patterns.
2. Validates each record against the `SpotifyExportRecord` schema.
3. Filters out podcasts (null `track_name` or `artist_name`).
4. Uploads the original raw bytes to Bronze to preserve source data exactly as exported.

### 3. Silver Transform (`pipelines/silver.py`)

The core transformation pipeline that reads all Bronze sources and produces a Silver star schema tables in Parquet.

**Data sources ingested:**
- API ingest (recently-played)
- Spotify export (standard and extended formats)
- Spotify enrichment (resolved track IDs)
- Last.fm enrichment (tags, listeners, playcount)
- Last.fm artist enrichment (artist tags)

**Output star schema:**

| Table | Type | Notes |
|---|---|---|
| `artists` | Dimension | artist_id, name |
| `albums` | Dimension | album_id, name, type, release_date, images |
| `tracks` | Dimension | track_id, name, duration, popularity, ISRC |
| `lastfm` | Dimension | track-level tags, listeners, playcount |
| `lastfm_artists` | Dimension | artist-level tags |
| `listening_events` | Fact | Partitioned by year/month |


**Key logic:**
- Handles nested JSON schema changes across API files using `infer_schema_length=None`.
- Detects export format (standard vs extended) and parses accordingly.
- Deduplicates events by source priority: API (1) > extended export (2) > standard export (3).
- Preserves enrichment columns from prior runs when re-transforming.

### 4. Enrichment (`pipelines/enrichment.py`)

Resolves missing track IDs by searching the Spotify API:
1. Compares fact table track IDs against the tracks dimension to find gaps.
2. Skips track IDs already attempted (tracked via base64-encoded filenames in Bronze).
3. Fetches tracks in batches with a 35s sleep between requests to respect rate limits.
4. Stores results with `search_artist_name`/`search_track_name` for the Silver join.

This pipeline is slow and meant to run locally, not in scheduled jobs. This slowness is the results of Spotify restricting access to their API so we need to put a very long idle time between requests.

### 5. Last.fm Enrichment (`pipelines/lastfm_enrichment.py`)

Two phases:

**Phase 1. Track enrichment:** Reads `(track_id, track_name, artist_name)` from Silver, fetches Last.fm track info (tags, listeners, playcount), and saves to Bronze. Uses 1s sleep between requests (even 0,5s works).

**Phase 2. Artist enrichment:** Reads `(artist_id, artist_name)` from Silver artists, fetches Last.fm artist top tags, and saves to Bronze. Artist level tags tend to be better populated than track-level ones.

### 6. Dump Enrichment (`pipelines/dump_enrichment.py`)

Enriches Silver data using local Spotify catalogue dumps in five phases:

| Phase | Purpose |
|---|---|
| 1 - Track ID resolution | Matches `(track_name, artist_name)` pairs with null track_id against the dump |
| 2 - Track detail fill | Adds ISRC, popularity, disc_number from dump to Silver tracks |
| 3 - Artist enrichment | Adds followers, popularity, genres from dump to Silver artists |
| 4 - Album enrichment | Adds label, UPC, popularity from dump to Silver albums |
| 5 - Audio features | Loads audio features (tempo, danceability, energy, valence, etc.) from dump |

This pipeline runs locally only because the dumps are ~100GB and not uploaded to cloud storage. Ideally, one runs it regularly to enrich new listening events that we observe for the first time.

### 7. Gold Analytics (`pipelines/gold.py`)

Builds the analytics layer from Silver that consists of dimensions, a wide fact table, and pre-computed aggregations.

**Dimensions:**
- `dim_tracks`: Tracks joined with album metadata, Last.fm track tags, and audio features.
- `dim_artists`: Artists joined with Last.fm artist tags, Spotify genres, a derived `primary_genre`, and `combined_genres`.

**Fact table:**
- `fact_plays`: Wide table joining listening events with all metadata. Includes:
  - Time dimensions: `played_year`, `played_month`, `played_day`, `played_hour`, `played_dow`, `is_weekend`.
  - Completion metrics: `completion_pct`, `is_complete` (for exports only).
  - Playback metadata: `platform`, `conn_country`, `shuffle`, `skipped`, `offline`, `incognito_mode`.
  - Audio features: `danceability`, `energy`, `valence`, `tempo`, `acousticness`, etc.
  - Derived: `mood_quadrant` (Euphoric/Intense/Chill/Melancholic from energy x valence).
  - Partitioned by year/month.

**Aggregations:**

| Table | Grain | Key Metrics |
|---|---|---|
| `agg_daily` | Calendar day | total_plays, total_ms_listened, unique_tracks, unique_artists, complete_plays, skip_count, avg_energy/valence/danceability, top_artist, top_genre |
| `agg_monthly` | Year + month | Same as daily + new_artists (first time heard that month) |
| `agg_artist_stats` | Artist (lifetime) | total_plays, unique_tracks, first/last_played_at, skip_rate, primary_genre |
| `agg_track_stats` | Track (lifetime) | total_plays, first/last_played_at, skip_rate, energy/valence/danceability |

---

## Data Flow

```
Spotify API              Spotify Exports            Last.fm API            Local Dumps
    |                          |                        |                       |
    v                          v                        v                       v
[Ingestion]             [Backfill]             [LastfmEnrichment]      [DumpEnrichment]
    |                          |                        |                       |
    +---------> Bronze <-------+------------------------+-----------------------+
                   |
            [SilverPipeline]
            (dedup, star schema, enrichment merge)
                   |
                Silver
                   |
             [GoldPipeline]
   (joins, aggregations, derived metrics)
                   |
                 Gold
```

---

## Core Design Choices

- **Polars over Pandas**: Polars is used for all DataFrame operations. It is significantly faster for the scan-and-transform workloads in this pipeline, especially when reading partitioned Parquet with predicate pushdown.
- **Pydantic for validation**: All external data (API responses, exports) is validated through Pydantic models before entering Bronze, catching schema issues early.
- **Tenacity for retries**: Spotify API calls use exponential backoff (2-10s, 3 retries). Last.fm calls do not retry since the API is more reliable.
- **Hive partitioning**: Bronze and Silver fact tables use `year=/month=/day=` partitioning for efficient time-range queries.
- **Source-priority deduplication**: When the same play appears in multiple sources (API, standard export, extended export), the highest-fidelity source wins (API > extended > standard).
- **Base64-encoded filenames**: Enrichment results use base64-encoded keys as blob names to avoid filesystem-unsafe characters.
- **ThreadPoolExecutor (64 workers)**: Used for downloading enrichment files from Bronze in parallel.
- **Single Settings singleton**: One configuration object with computed properties for each framework's storage options (fsspec, Polars), so auth logic is centralized.
