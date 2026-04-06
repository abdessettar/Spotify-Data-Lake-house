# Data Model: Silver Layer

Cleaned, deduplicated, and normalized data modeled  as a star schema with dimension and fact tables, sttored as Parquet on Azure Blob Storage.
This layer is built by `SilverPipeline` (`src/silver.py`).

---

## 1. artists

Artist dimension: one row per unique artist.

**Path:** `silver/artists/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `artist_id` | String | Spotify artist ID (PK) | bronze/spotify_api `track.artists[].id` and bronze/spotify_enrichment `track.artists[].id` |
| 2 | `artist_name` | String | Artist name | bronze/spotify_api `track.artists[].name` and bronze/spotify_enrichment `track.artists[].name` |
| 3 | `artist_uri` | String | Spotify URI (`spotify:artist:{id}`) | bronze/spotify_api `track.artists[].uri` and bronze/spotify_enrichment `track.artists[].uri` |
| 4 | `genres` | List(String) | Spotify genre taxonomy | Dump enrichment (phase 3)  |
| 5 | `followers_total` | Int64 | Total Spotify followers | Dump enrichment (phase 3)  |
| 6 | `popularity` | Int64 | Spotify popularity 0–100 | Dump enrichment (phase 3)  |
| 7 | `genres_right` | List(String) | Duplicate of `genres` (join artifact) | Dump enrichment join artifact |

Artists are extracted by unnesting `track.artists[]` from API and enrichment responses, then deduplicated by `artist_id`.

---

## 2. albums

Album dimension: one row per unique album.

**Path:** `silver/albums/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `album_id` | String | Spotify album ID (PK) | bronze/spotify_api `track.album.id` and bronze/spotify_enrichment `track.album.id` |
| 2 | `album_name` | String | Album title | bronze/spotify_api `track.album.name` and bronze/spotify_enrichment `track.album.name` |
| 3 | `album_type` | String | `"album"`, `"single"`, or `"compilation"` | bronze/spotify_api `track.album.album_type` and bronze/spotify_enrichment `track.album.album_type` |
| 4 | `release_date` | String | Release date (ISO 8601, precision varies: YYYY, YYYY-MM, or YYYY-MM-DD) | bronze/spotify_api `track.album.release_date` and bronze/spotify_enrichment `track.album.release_date` |
| 5 | `total_tracks` | Int64 | Number of tracks on the album | bronze/spotify_api `track.album.total_tracks` and bronze/spotify_enrichment `track.album.total_tracks` |
| 6 | `image_url` | String | Largest album artwork URL | bronze/spotify_api `track.album.images[0].url` (first = largest) |
| 7 | `label` | String | Record label | Dump enrichment (phase 4)  |
| 8 | `upc` | String | Universal Product Code | Dump enrichment (phase 4)  |
| 9 | `popularity` | Int64 | Album popularity 0–100 | Dump enrichment (phase 4)  |
| 10 | `label_right` | String | Duplicate of `label` (join artifact) | Dump enrichment join artifact |
| 11 | `upc_right` | String | Duplicate of `upc` (join artifact) | Dump enrichment join artifact |

Albums are extracted from `track.album` in API and enrichment responses, then deduplicated by `album_id`.

---

## 3. tracks

Track dimension: one row per unique track.

**Path:** `silver/tracks/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `track_id` | String | Spotify track ID (PK) | bronze/spotify_api `track.id` and bronze/spotify_enrichment `track.id` |
| 2 | `track_name` | String | Track title | bronze/spotify_api `track.name` and bronze/spotify_enrichment `track.name` |
| 3 | `duration_ms` | Int64 | Full track duration in ms | bronze/spotify_api `track.duration_ms` and bronze/spotify_enrichment `track.duration_ms` |
| 4 | `explicit` | Boolean | Has explicit content | bronze/spotify_api `track.explicit` and bronze/spotify_enrichment `track.explicit` |
| 5 | `popularity` | Int64 | Track popularity 0–100 | bronze/spotify_api `track.popularity` or dump enrichment (phase 2) |
| 6 | `track_number` | Int64 | Position on the album | bronze/spotify_api `track.track_number` and bronze/spotify_enrichment `track.track_number` |
| 7 | `album_id` | String | FK to albums dimension | bronze/spotify_api `track.album.id` and bronze/spotify_enrichment `track.album.id` |
| 8 | `track_uri` | String | Spotify URI (`spotify:track:{id}`) | bronze/spotify_api `track.uri` and bronze/spotify_enrichment `track.uri` |
| 9 | `isrc` | String | International Standard Recording Code | bronze/spotify_enrichment `track.external_ids.isrc` or dump enrichment (phase 2) |
| 10 | `disc_number` | Int64 | Disc number for multi-disc albums | Dump enrichment (phase 2)  |

Tracks are extracted from API and enrichment responses, then deduplicated by `track_id`.

---

## 4. lastfm

Last.fm track tags dimension: one row per track.

**Path:** `silver/lastfm/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `track_id` | String | Spotify track ID (PK) | bronze/lastfm_enrichment `track_id` |
| 2 | `tags` | List(String) | Up to 5 genre/style tags (noise filtered). Can be empty. | bronze/lastfm_enrichment `tags` |
| 3 | `top_tag` | String | Most relevant tag (first in list). Null if no tags. | bronze/lastfm_enrichment `top_tag` |
| 4 | `listeners` | Int64 | Global unique listeners on Last.fm | bronze/lastfm_enrichment `listeners` |
| 5 | `playcount` | Int64 | Global total plays on Last.fm | bronze/lastfm_enrichment `playcount` |

Direct read from bronze lastfm_enrichment JSON files, loaded into a single Parquet table.

---

## 5. lastfm_artists

Last.fm artist tags dimension: one row per artist.

**Path:** `silver/lastfm_artists/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `artist_id` | String | Spotify artist ID (PK) | bronze/lastfm_artist_enrichment `artist_id` |
| 2 | `tags` | List(String) | Up to 5 genre/style tags (noise filtered) | bronze/lastfm_artist_enrichment `tags` |
| 3 | `top_tag` | String | Most relevant tag (first in list) | bronze/lastfm_artist_enrichment `top_tag` |

Direct read from bronze lastfm_artist_enrichment JSON files, loaded into a single Parquet table. Artist tags are usually better populated than track tags, so this is the main genre source.

---

## 6. audio_features

Spotify audio features dimension: one row per track.

**Path:** `silver/audio_features/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `track_id` | String | Spotify track ID (PK) | Dump enrichment (phase 5) — offline Spotify audio_features dump |
| 2 | `time_signature` | Int64 | Beats per measure (3, 4, 5, 7) | Dump enrichment (phase 5) |
| 3 | `tempo` | Float64 | Speed in BPM | Dump enrichment (phase 5) |
| 4 | `key` | Int64 | Musical key (0=C, 1=C#, ... 11=B) | Dump enrichment (phase 5) |
| 5 | `mode` | Int64 | 0=minor, 1=major | Dump enrichment (phase 5) |
| 6 | `danceability` | Float64 | Danceability (0.0–1.0) | Dump enrichment (phase 5) |
| 7 | `energy` | Float64 | Intensity (0.0–1.0) | Dump enrichment (phase 5) |
| 8 | `loudness` | Float64 | Volume in dB | Dump enrichment (phase 5) |
| 9 | `speechiness` | Float64 | Spoken words (0.0–1.0) | Dump enrichment (phase 5) |
| 10 | `acousticness` | Float64 | Acoustic vs electronic (0.0–1.0) | Dump enrichment (phase 5) |
| 11 | `instrumentalness` | Float64 | Chance of no vocals (0.0–1.0) | Dump enrichment (phase 5) |
| 12 | `liveness` | Float64 | Live recording chance (0.0–1.0) | Dump enrichment (phase 5) |
| 13 | `valence` | Float64 | Musical positiveness (0.0–1.0) | Dump enrichment (phase 5) |

Spotify removed the audio features API in 2023, but we found a dump online which we used to fill these. Filtered to tracks that exist in the silver tracks table, rows with null responses are excluded.

---

## 7. listening_events

Fact table: one row per play event. Hive-partitioned by year/month.

**Path:** `silver/listening_events/year={Y}/month={MM}/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `played_at_utc` | Datetime(us) | UTC timestamp of play | bronze/spotify_api `played_at` or bronze/spotify_export `ts` (extended) or `endTime` (standard, converted to UTC) |
| 2 | `track_id` | String | Spotify track ID | bronze/spotify_api `track.id` or bronze/spotify_export `spotify_track_uri` (extracted) or bronze/spotify_enrichment mapping (for standard export) |
| 3 | `track_name` | String | Track title | bronze/spotify_api `track.name` or bronze/spotify_export `master_metadata_track_name` (extended) or `trackName` (standard) |
| 4 | `artist_name` | String | Primary artist name | bronze/spotify_api `track.artists[0].name` or bronze/spotify_export `master_metadata_album_artist_name` (extended) or `artistName` (standard) |
| 5 | `duration_ms` | Int64 | Ms the user actually listened | bronze/spotify_api `track.duration_ms` or bronze/spotify_export `ms_played` (extended) or `msPlayed` (standard) |
| 6 | `context_type` | String | Playback context: "playlist", "album", etc. API only. | bronze/spotify_api `context.type` |
| 7 | `context_uri` | String | Spotify URI of context. API only. | bronze/spotify_api `context.uri` |
| 8 | `source_type` | String | `"api"`, `"extended_export"`, or `"export"` | Derived — set during ingestion based on the source file format |
| 9 | `platform` | String | Device (e.g. "ios"). Export only. | bronze/spotify_export `platform` |
| 10 | `conn_country` | String | 2-letter country code. Export only. | bronze/spotify_export `conn_country` |
| 11 | `reason_start` | String | Why playback started. Export only. | bronze/spotify_export `reason_start` |
| 12 | `reason_end` | String | Why playback ended. Export only. | bronze/spotify_export `reason_end` |
| 13 | `shuffle` | Boolean | Shuffle mode on. Export only. | bronze/spotify_export `shuffle` |
| 14 | `skipped` | Boolean | User skipped. Export only. | bronze/spotify_export `skipped` |
| 15 | `offline` | Boolean | Offline mode. Export only. | bronze/spotify_export `offline` |
| 16 | `incognito_mode` | Boolean | Private session. Export only. | bronze/spotify_export `incognito_mode` |
| 17 | `album_name` | String | Album name. Extended export only. | bronze/spotify_export `master_metadata_album_album_name` |
| 18 | `year` | Int64 | Hive partition: year from `played_at_utc` | Derived from `played_at_utc` |
| 19 | `month` | Int64 | Hive partition: month from `played_at_utc` | Derived from `played_at_utc` |

### Business Rules

- Events from all three sources (API, extended export, standard export) are combined into one table.
- Deduplication: if the same track was played at the same time and appears in multiple sources, only the highest-priority source is kept. Priority: `api` (1) > `extended_export` (2) > `export` (3).
- `track_id` can be null for old standard export records where no enrichment match was found.
- For API events, `duration_ms` is the full track duration (not actual listening time). For export events, it is actual listening time.
- Columns 6–7 (context) are only filled for API events. Columns 9–16 (behavioral) are only filled for export events. Column 17 (album_name) is only filled for extended export events.

---

## Lineage Diagram

```
              BRONZE LAYER
  ┌──────────────────────────────────────────────────┐
  │  spotify_api/         spotify_export/            │
  │  spotify_enrichment/  lastfm_enrichment/         │
  │                       lastfm_artist_enrichment/  │
  │                       + offline dump             │
  └────────────┬─────────────────────────────────────┘
               │
               v
         SilverPipeline
               │
     ┌─────────┼─────────────────────────────────────┐
     │         │         │         │        │        │
     v         v         v         v        v        v
  artists   albums    tracks    lastfm  lastfm_   audio_
                                        artists   features
     │         │         │
     └─────────┼─────────┘
               │
               v (combined with enrichment mapping)
        listening_events
```
