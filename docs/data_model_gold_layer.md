# Data Model: Gold Layer

Analytics-ready tables built from the Silver layer and stored as Parquet files on Azure Blob Storage. <br>
Built by `GoldPipeline` (`src/gold.py`).

---

## 1. dim_tracks

Wide track dimension that is the results of joining tracks with albums, audio features, and Last.fm tags.

**Path:** `gold/dim_tracks/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `track_id` | String | Spotify track ID (PK) | silver/tracks |
| 2 | `track_name` | String | Track title | silver/tracks |
| 3 | `duration_ms` | Int64 | Full track duration in ms | silver/tracks |
| 4 | `explicit` | Boolean | Has explicit content | silver/tracks |
| 5 | `popularity` | Int64 | Track popularity 0–100 | silver/tracks (from dump enrichment) |
| 6 | `track_number` | Int64 | Position on the album | silver/tracks |
| 7 | `album_id` | String | FK to album | silver/tracks |
| 8 | `track_uri` | String | Spotify URI (`spotify:track:{id}`) | silver/tracks |
| 9 | `isrc` | String | International Standard Recording Code | silver/tracks (from enrichment or dump) |
| 10 | `disc_number` | Int64 | Disc number for multi-disc albums | silver/tracks (from dump enrichment) |
| 11 | `album_name` | String | Album title | silver/albums → join on `album_id` |
| 12 | `album_type` | String | `"album"`, `"single"`, or `"compilation"` | silver/albums → join on `album_id` |
| 13 | `release_date` | String | Release date (ISO 8601, precision varies) | silver/albums → join on `album_id` |
| 14 | `total_tracks` | Int64 | Number of tracks on the album | silver/albums → join on `album_id` |
| 15 | `image_url` | String | Largest album artwork URL | silver/albums → join on `album_id` |
| 16 | `album_label` | String | Record label | silver/albums → join on `album_id` (from dump enrichment) |
| 17 | `album_upc` | String | Universal Product Code | silver/albums → join on `album_id` (from dump enrichment) |
| 18 | `album_popularity` | Int64 | Album popularity 0–100 | silver/albums → join on `album_id` (from dump enrichment) |
| 19 | `label_right` | String | Duplicate of `album_label` (join artifact) | silver/albums → join artifact |
| 20 | `upc_right` | String | Duplicate of `album_upc` (join artifact) | silver/albums → join artifact |
| 21 | `release_year` | Int32 | Year extracted from `release_date` | Derived from `release_date` |
| 22 | `lastfm_tags` | List(String) | Up to 5 genre/style tags | silver/lastfm → join on `track_id` |
| 23 | `lastfm_top_tag` | String | Most relevant tag | silver/lastfm → join on `track_id` |
| 24 | `lastfm_listeners` | Int64 | Global unique listeners on Last.fm | silver/lastfm → join on `track_id` |
| 25 | `lastfm_playcount` | Int64 | Global total plays on Last.fm | silver/lastfm → join on `track_id` |
| 26 | `time_signature` | Int64 | Beats per measure (3, 4, 5, 7) | silver/audio_features → join on `track_id` |
| 27 | `tempo` | Float64 | Speed in BPM | silver/audio_features → join on `track_id` |
| 28 | `key` | Int64 | Musical key (0=C, 1=C#, ... 11=B) | silver/audio_features → join on `track_id` |
| 29 | `mode` | Int64 | 0=minor, 1=major | silver/audio_features → join on `track_id` |
| 30 | `danceability` | Float64 | Danceability score (0.0–1.0) | silver/audio_features → join on `track_id` |
| 31 | `energy` | Float64 | Intensity level (0.0–1.0) | silver/audio_features → join on `track_id` |
| 32 | `loudness` | Float64 | Volume in dB | silver/audio_features → join on `track_id` |
| 33 | `speechiness` | Float64 | Spoken words amount (0.0–1.0) | silver/audio_features → join on `track_id` |
| 34 | `acousticness` | Float64 | Acoustic vs electronic (0.0–1.0) | silver/audio_features → join on `track_id` |
| 35 | `instrumentalness` | Float64 | Chance of no vocals (0.0–1.0) | silver/audio_features → join on `track_id` |
| 36 | `liveness` | Float64 | Live recording chance (0.0–1.0) | silver/audio_features → join on `track_id` |
| 37 | `valence` | Float64 | Musical positiveness (0.0–1.0) | silver/audio_features → join on `track_id` |

All joins are LEFT joins from tracks, so every track is present even if album, audio features, or tags are missing.

---

## 2. dim_artists

Wide artist dimension that combines Spotify metadata with Last.fm genre tags.

**Path:** `gold/dim_artists/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `artist_id` | String | Spotify artist ID (PK) | silver/artists |
| 2 | `artist_name` | String | Artist name | silver/artists |
| 3 | `artist_uri` | String | Spotify URI (`spotify:artist:{id}`) | silver/artists |
| 4 | `genres` | List(String) | Spotify genre list | silver/artists (from dump enrichment) |
| 5 | `followers_total` | Int64 | Total Spotify followers | silver/artists (from dump enrichment) |
| 6 | `popularity` | Int64 | Spotify popularity 0–100 | silver/artists (from dump enrichment) |
| 7 | `genres_right` | List(String) | Duplicate of `genres` (join artifact) | silver/artists → join artifact |
| 8 | `primary_genre` | String | First element of `genres` | Derived from `genres[0]` |
| 9 | `lastfm_tags` | List(String) | Up to 5 Last.fm genre tags | silver/lastfm_artists → join on `artist_id` |
| 10 | `lastfm_top_tag` | String | Most relevant Last.fm tag | silver/lastfm_artists → join on `artist_id` |
| 11 | `combined_genres` | List(String) | Deduplicated union of `genres` + `lastfm_tags` | Derived from `genres` ∪ `lastfm_tags` |

---

## 3. fact_plays

Main fact table: one row per play event, enriched with all dimensions and Hive-partitioned by year/month.

**Path:** `gold/fact_plays/year={Y}/month={MM}/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `played_at_utc` | Datetime(us) | UTC timestamp of play | silver/listening_events |
| 2 | `track_id` | String | Spotify track ID | silver/listening_events |
| 3 | `track_name` | String | Track title | silver/listening_events |
| 4 | `artist_name` | String | Primary artist name | silver/listening_events |
| 5 | `duration_ms_played` | Int64 | Actual ms the user listened | silver/listening_events.duration_ms (renamed) |
| 6 | `context_type` | String | Playback context: "playlist", "album", etc. API source only. | silver/listening_events |
| 7 | `context_uri` | String | Spotify URI of context. API source only. | silver/listening_events |
| 8 | `source_type` | String | `"api"`, `"extended_export"`, or `"export"` | silver/listening_events |
| 9 | `platform` | String | Device (e.g. "ios"). Export only. | silver/listening_events |
| 10 | `conn_country` | String | 2-letter country code. Export only. | silver/listening_events |
| 11 | `reason_start` | String | Why playback started. Export only. | silver/listening_events |
| 12 | `reason_end` | String | Why playback ended. Export only. | silver/listening_events |
| 13 | `shuffle` | Boolean | Shuffle mode on. Export only. | silver/listening_events |
| 14 | `skipped` | Boolean | User skipped. Export only. | silver/listening_events |
| 15 | `offline` | Boolean | Offline mode. Export only. | silver/listening_events |
| 16 | `incognito_mode` | Boolean | Private session. Export only. | silver/listening_events |
| 17 | `album_name` | String | Album name. Extended export only. | silver/listening_events |
| 18 | `year` | Int64 | Hive partition: year | silver/listening_events |
| 19 | `month` | Int64 | Hive partition: month | silver/listening_events |
| 20 | `played_year` | Int32 | Year of play | Derived from `played_at_utc` |
| 21 | `played_month` | Int32 | Month (1–12) | Derived from `played_at_utc` |
| 22 | `played_day` | Int32 | Day of month | Derived from `played_at_utc` |
| 23 | `played_hour` | Int32 | Hour (0–23) | Derived from `played_at_utc` |
| 24 | `played_dow` | Int32 | Day of week (1=Mon, 7=Sun) | Derived from `played_at_utc` |
| 25 | `is_weekend` | Boolean | True if Sat or Sun (`played_dow >= 6`) | Derived from `played_dow` |
| 26 | `album_id` | String | FK to album | silver/tracks → join on `track_id` |
| 27 | `duration_ms_track` | Int64 | Full track duration in ms | silver/tracks → join on `track_id` |
| 28 | `explicit` | Boolean | Has explicit content | silver/tracks → join on `track_id` |
| 29 | `track_popularity` | Int64 | Track popularity 0–100 | silver/tracks → join on `track_id` |
| 30 | `isrc` | String | ISRC code | silver/tracks → join on `track_id` |
| 31 | `album_type` | String | `"album"`, `"single"`, or `"compilation"` | silver/albums → join on `album_id` |
| 32 | `release_date` | String | Release date (ISO 8601) | silver/albums → join on `album_id` |
| 33 | `album_popularity` | Int64 | Album popularity 0–100 | silver/albums → join on `album_id` |
| 34 | `album_label` | String | Record label | silver/albums → join on `album_id` |
| 35 | `release_year` | Int32 | Year from `release_date` | Derived from `release_date` |
| 36 | `completion_pct` | Float64 | `duration_ms_played / duration_ms_track`. Export only. | Derived |
| 37 | `is_complete` | Boolean | `duration_ms_played >= 30000` (Spotify stream threshold). Export only. | Derived |
| 38 | `artist_id` | String | Spotify artist ID | silver/artists → join on `artist_name` |
| 39 | `top_genre` | String | Top genre tag for the artist | silver/lastfm_artists → join on `artist_id` |
| 40 | `danceability` | Float64 | Danceability (0.0–1.0) | silver/audio_features → join on `track_id` |
| 41 | `energy` | Float64 | Intensity (0.0–1.0) | silver/audio_features → join on `track_id` |
| 42 | `valence` | Float64 | Positiveness (0.0–1.0) | silver/audio_features → join on `track_id` |
| 43 | `tempo` | Float64 | BPM | silver/audio_features → join on `track_id` |
| 44 | `acousticness` | Float64 | Acoustic (0.0–1.0) | silver/audio_features → join on `track_id` |
| 45 | `instrumentalness` | Float64 | Instrumental (0.0–1.0) | silver/audio_features → join on `track_id` |
| 46 | `speechiness` | Float64 | Spoken words (0.0–1.0) | silver/audio_features → join on `track_id` |
| 47 | `loudness` | Float64 | Volume in dB | silver/audio_features → join on `track_id` |
| 48 | `liveness` | Float64 | Live recording (0.0–1.0) | silver/audio_features → join on `track_id` |
| 49 | `key` | Int64 | Musical key (0–11) | silver/audio_features → join on `track_id` |
| 50 | `mode` | Int64 | 0=minor, 1=major | silver/audio_features → join on `track_id` |
| 51 | `time_signature` | Int64 | Beats per measure | silver/audio_features → join on `track_id` |
| 52 | `mood_quadrant` | String | `"Euphoric"`, `"Intense"`, `"Chill"`, or `"Melancholic"` | Derived from `energy` + `valence` |

### Mood Quadrant Logic
We keep it simple and approximate, as this is not an exact science and can become quiet complexe ([1](https://rpubs.com/mary18/860196), [2](https://pmc.ncbi.nlm.nih.gov/articles/PMC9522267/)).

|  | valence > 0.5 | valence <= 0.5 |
|--|---------------|----------------|
| **energy > 0.5** | Euphoric | Intense |
| **energy <= 0.5** | Chill | Melancholic |

### Business Rules

- Deduplication happens in Silver: priority is api > extended_export > export.
- `artist_id` is joined on `artist_name` (not ID), because Silver events only have the name.
- `completion_pct` and `is_complete` are null for API events.
- Audio features are null for tracks not in the offline dump (can't do much about it).

---

## 4. agg_daily

Daily listening summary: onne row per day.

**Path:** `gold/agg_daily/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `date` | Date | Calendar date | fact_plays.played_at_utc → `.date()` |
| 2 | `total_plays` | UInt32 | Plays that day | `count(*)` over fact_plays |
| 3 | `total_ms_listened` | Int64 | Total listening time in ms | `sum(duration_ms_played)` |
| 4 | `unique_tracks` | UInt32 | Different tracks played | `count(distinct track_id)` |
| 5 | `unique_artists` | UInt32 | Different artists played | `count(distinct artist_name)` |
| 6 | `complete_plays` | Int64 | Plays >= 30s. Export only. | `sum(is_complete)` |
| 7 | `skip_count` | Int64 | Skipped plays. Export only. | `sum(skipped)` |
| 8 | `avg_energy` | Float64 | Mean energy that day | `mean(energy)` |
| 9 | `avg_valence` | Float64 | Mean valence that day | `mean(valence)` |
| 10 | `avg_danceability` | Float64 | Mean danceability that day | `mean(danceability)` |
| 11 | `top_artist` | String | Most played artist | Most frequent `artist_name` |
| 12 | `top_genre` | String | Most played genre | Most frequent `top_genre` |

---

## 5. agg_monthly

Monthly listening summary: one row per month.

**Path:** `gold/agg_monthly/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `year` | Int32 | Calendar year | fact_plays.played_at_utc → `.year()` |
| 2 | `month` | Int32 | Calendar month (1–12) | fact_plays.played_at_utc → `.month()` |
| 3 | `total_plays` | UInt32 | Plays that month | `count(*)` |
| 4 | `total_ms_listened` | Int64 | Total listening time in ms | `sum(duration_ms_played)` |
| 5 | `unique_tracks` | UInt32 | Different tracks played | `count(distinct track_id)` |
| 6 | `unique_artists` | UInt32 | Different artists played | `count(distinct artist_name)` |
| 7 | `complete_plays` | Int64 | Plays >= 30s. Export only. | `sum(is_complete)` |
| 8 | `skip_count` | Int64 | Skipped plays. Export only. | `sum(skipped)` |
| 9 | `avg_energy` | Float64 | Mean energy that month | `mean(energy)` |
| 10 | `avg_valence` | Float64 | Mean valence that month | `mean(valence)` |
| 11 | `avg_danceability` | Float64 | Mean danceability that month | `mean(danceability)` |
| 12 | `new_artists` | UInt32 | Artists heard for the first time this month | Count of artists where `min(played_at_utc)` falls in this month |
| 13 | `top_artist` | String | Most played artist | Most frequent `artist_name` |

---

## 6. agg_artist_stats

Lifetime stats per artist, sorted by `total_plays` DESC.

**Path:** `gold/agg_artist_stats/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `artist_name` | String | Artist name | fact_plays.artist_name (group key) |
| 2 | `total_plays` | UInt32 | Lifetime plays | `count(*)` over fact_plays |
| 3 | `total_ms_listened` | Int64 | Lifetime listening time in ms | `sum(duration_ms_played)` |
| 4 | `unique_tracks` | UInt32 | Different tracks played | `count(distinct track_id)` |
| 5 | `first_played_at` | Datetime(us) | First time heard | `min(played_at_utc)` |
| 6 | `last_played_at` | Datetime(us) | Most recent play | `max(played_at_utc)` |
| 7 | `avg_energy` | Float64 | Mean energy across plays | `mean(energy)` |
| 8 | `avg_valence` | Float64 | Mean valence across plays | `mean(valence)` |
| 9 | `avg_danceability` | Float64 | Mean danceability across plays | `mean(danceability)` |
| 10 | `skip_count` | Int64 | Times skipped. Export only. | `sum(skipped)` |
| 11 | `skip_rate` | Float64 | Skip percentage. Export only. | `skip_count / total_plays` |
| 12 | `artist_id` | String | Spotify artist ID | gold/dim_artists → join on `artist_name` |
| 13 | `primary_genre` | String | First Spotify genre | gold/dim_artists → join on `artist_name` |
| 14 | `followers_total` | Int64 | Spotify followers | gold/dim_artists → join on `artist_name` |
| 15 | `popularity` | Int64 | Spotify popularity 0–100 | gold/dim_artists → join on `artist_name` |
| 16 | `lastfm_top_tag` | String | Top Last.fm genre tag | gold/dim_artists → join on `artist_name` |

---

## 7. agg_track_stats

Lifetime stats per track; sorted by `total_plays` DESC.

**Path:** `gold/agg_track_stats/data.parquet`

| # | Column | Data Type | Description | Source |
|---|--------|-----------|-------------|--------|
| 1 | `track_id` | String | Spotify track ID | fact_plays.track_id (group key) |
| 2 | `track_name` | String | Track title | fact_plays.track_name (group key) |
| 3 | `artist_name` | String | Primary artist | fact_plays.artist_name (group key) |
| 4 | `total_plays` | UInt32 | Lifetime plays | `count(*)` over fact_plays |
| 5 | `total_ms_listened` | Int64 | Lifetime listening time in ms | `sum(duration_ms_played)` |
| 6 | `first_played_at` | Datetime(us) | First time played | `min(played_at_utc)` |
| 7 | `last_played_at` | Datetime(us) | Most recent play | `max(played_at_utc)` |
| 8 | `album_name` | String | Album name | `first(album_name)` |
| 9 | `energy` | Float64 | Energy (0.0–1.0) | `first(energy)` |
| 10 | `valence` | Float64 | Valence (0.0–1.0) | `first(valence)` |
| 11 | `danceability` | Float64 | Danceability (0.0–1.0) | `first(danceability)` |
| 12 | `popularity` | Int64 | Track popularity 0–100 | `first(track_popularity)` |
| 13 | `skip_count` | Int64 | Times skipped. Export only. | `sum(skipped)` |
| 14 | `skip_rate` | Float64 | Skip percentage. Export only. | `skip_count / total_plays` |

---

## Lineage Diagram

```
                        SILVER LAYER
  ┌──────────────────────────────────────────────────────┐
  │  listening_events   tracks   albums   artists        │
  │  audio_features     lastfm   lastfm_artists          │
  └──────────┬───────────────────────────────────────────┘
             │
             v
       GoldPipeline
             │
  ┌──────────┼──────────────────────────────────┐
  │          │                                  │
  v          v                                  v
dim_tracks  dim_artists                     fact_plays
(tracks +   (artists +                      (events +
 albums +    lastfm_artists)                 all dims +
 audio +                                     derived
 lastfm)                                     fields)
                                                │
                            ┌─────────┬─────────┼──────────┐
                            v         v         v          v
                        agg_daily  agg_monthly  agg_       agg_
                                                artist_    track_
                                                stats      stats
```
