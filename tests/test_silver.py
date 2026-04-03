import polars as pl
from src.pipelines.silver import SilverPipeline


def test_extended_export_track_id_extraction():
    """Test that track_id is correctly extracted from spotify_track_uri in extended export data."""
    df = pl.DataFrame(
        {
            "ts": [
                "2018-06-13T15:55:30Z",
                "2018-06-14T10:00:00Z",
                "2018-06-15T12:00:00Z",
            ],
            "master_metadata_track_name": ["Music Box", "Lose Yourself", "Podcast Ep"],
            "master_metadata_album_artist_name": ["Eminem", "Eminem", None],
            "master_metadata_album_album_name": ["Relapse: Refill", "8 Mile", None],
            "ms_played": [304906, 200000, 50000],
            "spotify_track_uri": [
                "spotify:track:3JcC0WHJMBWNpzDR1Npfj7",
                "spotify:track:7MJQ9Nfxzh0LMF7Wnhlod3",
                "spotify:episode:abc123",
            ],
            "platform": ["OS X 10.13.5", "Android 13", "iOS 16"],
            "conn_country": ["BE", "US", "FR"],
            "reason_start": ["trackdone", "clickrow", "appload"],
            "reason_end": ["trackdone", "fwdbtn", "trackdone"],
            "shuffle": [False, True, False],
            "skipped": [False, True, False],
            "offline": [False, False, True],
            "incognito_mode": [False, False, True],
        }
    )

    # Apply the same transformation logic as SilverPipeline.run()
    result = df.select(
        [
            pl.col("ts")
            .str.replace("Z", "")
            .str.to_datetime(strict=False)
            .alias("played_at_utc"),
            pl.col("master_metadata_track_name").alias("track_name"),
            pl.col("master_metadata_album_artist_name").alias("artist_name"),
            pl.col("ms_played").alias("duration_ms").cast(pl.Int64),
            pl.when(
                pl.col("spotify_track_uri").is_not_null()
                & pl.col("spotify_track_uri").str.starts_with("spotify:track:")
            )
            .then(pl.col("spotify_track_uri").str.split(":").list.last())
            .otherwise(pl.lit(None).cast(pl.Utf8))
            .alias("track_id"),
            pl.lit("extended_export").alias("source_type"),
            pl.col("platform").cast(pl.Utf8),
            pl.col("conn_country").cast(pl.Utf8),
            pl.col("reason_start").cast(pl.Utf8),
            pl.col("reason_end").cast(pl.Utf8),
            pl.col("shuffle").cast(pl.Boolean),
            pl.col("skipped").cast(pl.Boolean),
            pl.col("offline").cast(pl.Boolean),
            pl.col("incognito_mode").cast(pl.Boolean),
            pl.col("master_metadata_album_album_name")
            .cast(pl.Utf8)
            .alias("album_name"),
        ]
    )

    # Track IDs extracted correctly
    assert result["track_id"][0] == "3JcC0WHJMBWNpzDR1Npfj7"
    assert result["track_id"][1] == "7MJQ9Nfxzh0LMF7Wnhlod3"
    assert result["track_id"][2] is None  # Episode URI → no track_id

    # Behavioral columns carried through
    assert result["platform"][0] == "OS X 10.13.5"
    assert result["conn_country"][0] == "BE"
    assert result["shuffle"][1] is True
    assert result["skipped"][1] is True
    assert result["offline"][2] is True
    assert result["incognito_mode"][2] is True
    assert result["reason_start"][1] == "clickrow"
    assert result["reason_end"][1] == "fwdbtn"
    assert result["album_name"][0] == "Relapse: Refill"

    # Source type
    assert result["source_type"][0] == "extended_export"


def test_extract_dimensions():
    """Test that nested Pydantic/JSON data is correctly flattened into the Star Schema dimensions."""
    # 1. Create a tiny mock DataFrame representing what Polars reads from the Bronze JSON
    mock_data = {
        "track": [
            {
                "id": "track_123",
                "name": "Dummy Track",
                "duration_ms": 200000,
                "explicit": False,
                "popularity": 85,
                "track_number": 1,
                "uri": "spotify:track:123",
                "album": {
                    "id": "album_456",
                    "name": "Dummy Album",
                    "album_type": "album",
                    "release_date": "2023-01-01",
                    "total_tracks": 10,
                    "images": [{"url": "http://image.com"}],
                },
                "artists": [
                    {
                        "id": "artist_789",
                        "name": "Test Artist",
                        "uri": "spotify:artists:789",
                    }
                ],
            }
        ]
    }

    df_raw = pl.DataFrame(mock_data)

    # 2. Run our pipeline's extraction method
    pipeline = SilverPipeline()
    df_artists, df_albums, df_tracks = pipeline._extract_dimensions(df_raw)

    # 3. Assert schemas and data were flattened correctly
    assert df_artists.height == 1
    assert df_artists["artist_name"][0] == "Test Artist"

    assert df_albums.height == 1
    assert df_albums["album_name"][0] == "Dummy Album"
    assert df_albums["total_tracks"][0] == 10  # Ensure it casted to Int correctly

    assert df_tracks.height == 1
    assert df_tracks["track_name"][0] == "Dummy Track"
    assert df_tracks["popularity"][0] == 85
