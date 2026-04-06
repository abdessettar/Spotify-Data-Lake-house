# Data Model: Bronze Layer

Raw data, no transformations to reflect the data in the source. <br>
Stored as JSON on Azure Blob Storage.

---

## 1. Spotify API — Recently Played

**Path:** `bronze/spotify_api/recently_played/year={Y}/month={MM}/day={DD}/{timestamp}.json`

Hive-partitioned by ingestion date and each file is a JSON array of play events ingested by `IngestionPipeline` (`src/ingestion.py`), which tracks its cursor in `system/state.json`.

### Play Event (root level)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `played_at` | String (ISO 8601, Z) | UTC timestamp of when the track was played |
| `context` | Object / null | Playback context (playlist, album, etc.). Null if played from search or queue. |
| `context.type` | String | Context type: "playlist", "album", "artist", etc. |
| `context.uri` | String | Spotify URI of the context |
| `context.href` | String | API URL of the context |
| `context.external_urls` | Object | External URLs (e.g. `{"spotify": "https://..."}`) |

### Track (`track.*`)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `track.id` | String | Spotify track ID |
| `track.name` | String | Track title |
| `track.duration_ms` | Integer | Duration in milliseconds |
| `track.explicit` | Boolean | Has explicit content |
| `track.popularity` | Integer | Popularity 0–100 |
| `track.track_number` | Integer | Position on the album |
| `track.type` | String | Always `"track"` |
| `track.uri` | String | Spotify URI (`spotify:track:{id}`) |
| `track.href` | String | API URL |
| `track.is_local` | Boolean | True if local file, not Spotify catalog |

### Album (`track.album.*`)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `track.album.id` | String | Spotify album ID |
| `track.album.name` | String | Album title |
| `track.album.album_type` | String | `"album"`, `"single"`, or `"compilation"` |
| `track.album.total_tracks` | Integer | Number of tracks on the album |
| `track.album.release_date` | String | Release date (precision varies: YYYY, YYYY-MM, or YYYY-MM-DD) |
| `track.album.release_date_precision` | String | `"year"`, `"month"`, or `"day"` |
| `track.album.type` | String | Always `"album"` |
| `track.album.uri` | String | Spotify URI |
| `track.album.href` | String | API URL |
| `track.album.images` | List[Object] | Artwork images, sorted largest first |
| `track.album.images[].url` | String | Image URL |
| `track.album.images[].height` | Integer | Height in pixels |
| `track.album.images[].width` | Integer | Width in pixels |
| `track.album.artists` | List[Object] | Album-level artists (same structure as track artists) |

### Artists (`track.artists[]`)

A track can have multiple artists.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `track.artists[].id` | String | Spotify artist ID |
| `track.artists[].name` | String | Artist name |
| `track.artists[].type` | String | Always `"artist"` |
| `track.artists[].uri` | String | Spotify URI |
| `track.artists[].href` | String | API URL |
| `track.artists[].external_urls` | Object | External URLs |

---

## 2. Spotify Export

**Path:** `bronze/spotify_export/{filename}.json`

Personal data export downloaded from Spotify account, with two possible formats: standard and extended (all time). This one is not partitioned.

### Standard Export (`StreamingHistory*.json`)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `endTime` | String | When the track stopped. Format `"YYYY-MM-DD HH:MM"`. **Local time, not UTC.** |
| `artistName` | String | Artist name |
| `trackName` | String | Track name |
| `msPlayed` | Integer | Milliseconds played |

No track ID in this format so tracks are matched later by artist name + track name.

### Extended Export (`Streaming_History_Audio_*.json` and `Streaming_History_Video_*.json`)

| Column | Data Type | Description |
|--------|-----------|-------------|
| `ts` | String (ISO 8601, Z) | UTC timestamp when the track stopped |
| `platform` | String | Device (e.g. `"ios"`, `"OS X 10.13.2 [x86 8]"`) |
| `ms_played` | Integer | Milliseconds of actual playback |
| `conn_country` | String | 2-letter country code (e.g. `"BE"`) |
| `ip_addr` | String | IP address at time of playback |
| `master_metadata_track_name` | String / null | Track name. Null for podcasts/videos. |
| `master_metadata_album_artist_name` | String / null | Primary artist. Null for podcasts/videos. |
| `master_metadata_album_album_name` | String / null | Album name. Null for podcasts/videos. |
| `spotify_track_uri` | String / null | Spotify URI (`spotify:track:{id}`). Can be null for old tracks. |
| `episode_name` | String / null | Podcast episode name. Null for music. |
| `episode_show_name` | String / null | Podcast show name. Null for music. |
| `spotify_episode_uri` | String / null | Episode URI. Null for music. |
| `audiobook_title` | String / null | Audiobook title. Null for music. |
| `audiobook_uri` | String / null | Audiobook URI. Null for music. |
| `audiobook_chapter_uri` | String / null | Audiobook chapter URI. Null for music. |
| `audiobook_chapter_title` | String / null | Audiobook chapter title. Null for music. |
| `reason_start` | String | Why playback started (`"trackdone"`, `"clickrow"`, `"appload"`, `"fwdbtn"`, ...) |
| `reason_end` | String | Why playback ended (`"trackdone"`, `"fwdbtn"`, `"backup"`, `"trackerror"`, ...) |
| `shuffle` | Boolean | Shuffle mode was on |
| `skipped` | Boolean | User skipped the track |
| `offline` | Boolean | Listened in offline mode |
| `offline_timestamp` | Integer / null | Unix timestamp of offline play. Null if online. |
| `incognito_mode` | Boolean | Private listening session |

### Files in storage
This only reflects the listening history for my current account and will vary for yours.

| File | Format |
|------|--------|
| `StreamingHistory0.json` — `StreamingHistory2.json` | Standard |
| `Streaming_History_Audio_2017-2018_0.json` — `Streaming_History_Audio_2025-2026_14.json` | Extended (audio) |
| `Streaming_History_Video_2017-2026.json` | Extended (video) |

---

## 3. Spotify Enrichment

**Path:** `bronze/spotify_enrichment/{base64(track_id)}.json`

Full track details fetched from Spotify API (`/v1/tracks/{id}`) for export tracks that are missing info. One file per track, ingested by `EnrichmentPipeline` (`src/enrichment.py`).

### When found

| Column | Data Type | Description |
|--------|-----------|-------------|
| `search_artist_name` | String | Artist name used to find this track |
| `search_track_name` | String | Track name used to find this track |
| `track` | Object | Full Spotify track object (same as Section 1, plus `external_ids` below) |
| `track.external_ids.isrc` | String | ISRC code (only present in enrichment responses, not in API ingest) |

### When not found

| Column | Data Type | Description |
|--------|-----------|-------------|
| `not_found` | Boolean | Always `true` |

---

## 4. Last.fm Track Enrichment

**Path:** `bronze/lastfm_enrichment/{base64(track_id)}.json`

Genre tags and global stats per track from Last.fm API (`track.getInfo`). One file per track, ingested by `LastfmEnrichmentPipeline` (`src/lastfm_enrichment.py`), rate-limited to 1s between requests as theri API os more accessible than Spotify's. Noise tags (e.g. "seen live", "favourite", "love") are filtered out.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `track_id` | String | Spotify track ID |
| `tags` | List[String] | Up to 5 genre/style tags (filtered). Can be empty but not Null |
| `top_tag` | String / null | Most relevant tag (first in list). Null if no tags. |
| `listeners` | Integer | Global unique listeners on Last.fm |
| `playcount` | Integer | Global total plays on Last.fm |

---

## 5. Last.fm Artist Enrichment

**Path:** `bronze/lastfm_artist_enrichment/{base64(artist_id)}.json`

Genre tags per artist from Last.fm API (`artist.getTopTags`). One file per artist, with same pipeline and rate limit as track above. As artist tags are usually better populated than track tags, this is the main source for genre info.

| Column | Data Type | Description |
|--------|-----------|-------------|
| `artist_id` | String | Spotify artist ID |
| `tags` | List[String] | Up to 5 genre/style tags (filtered) |
| `top_tag` | String | Most relevant tag (first in list) |

---

## Lineage Diagram

```
  Spotify API               Spotify Account             Last.fm API
  /me/player/recently-played (data export download)      track.getInfo / artist.getTopTags
       |                          |                           |
       v                          v                      +----+--------+
  IngestionPipeline         Manual upload                |             |
       |                          |                      v             v
       v                          v                 lastfm_        lastfm_artist_
  spotify_api/             spotify_export/           enrichment/    enrichment/
  recently_played/
                                  |
                    Spotify API   | (for tracks missing details)
                    /v1/tracks    |
                                  v
                           spotify_enrichment/

  All stored under: bronze/ in Azure Blob Storage container "spotify-data"
```
