import pytest
from pydantic import ValidationError
from src.domain.schemas import SpotifyExportRecord


def test_spotify_export_standard_format():
    """Test parsing the standard StreamingHistory.json format."""
    raw_data = {
        "endTime": "2026-03-10 07:57",
        "artistName": "Daft Punk",
        "trackName": "Harder, Better, Faster, Stronger",
        "msPlayed": 224640,
    }
    record = SpotifyExportRecord(**raw_data)
    assert record.played_at == "2026-03-10 07:57"
    assert record.artist_name == "Daft Punk"
    assert record.duration_ms == 224640


def test_spotify_export_extended_format():
    """Test parsing the lifetime endsong.json format."""
    raw_data = {
        "ts": "2026-03-10T07:57:00Z",
        "master_metadata_album_artist_name": "Daft Punk",
        "master_metadata_track_name": "Harder, Better, Faster, Stronger",
        "ms_played": 224640,
        "spotify_track_uri": "spotify:track:123",
    }
    record = SpotifyExportRecord(**raw_data)
    assert record.played_at == "2026-03-10T07:57:00Z"
    assert record.artist_name == "Daft Punk"
    assert record.duration_ms == 224640
    assert record.spotify_track_uri == "spotify:track:123"


def test_spotify_export_extended_format_full():
    """Test parsing extended format with all behavioral fields."""
    raw_data = {
        "ts": "2018-06-13T15:55:30Z",
        "platform": "OS X 10.13.5 [x86 8]",
        "ms_played": 304906,
        "conn_country": "BE",
        "master_metadata_track_name": "Music Box",
        "master_metadata_album_artist_name": "Eminem",
        "master_metadata_album_album_name": "Relapse: Refill",
        "spotify_track_uri": "spotify:track:3JcC0WHJMBWNpzDR1Npfj7",
        "reason_start": "trackdone",
        "reason_end": "trackdone",
        "shuffle": False,
        "skipped": False,
        "offline": False,
        "incognito_mode": False,
    }
    record = SpotifyExportRecord(**raw_data)
    assert record.platform == "OS X 10.13.5 [x86 8]"
    assert record.conn_country == "BE"
    assert record.master_metadata_album_album_name == "Relapse: Refill"
    assert record.reason_start == "trackdone"
    assert record.reason_end == "trackdone"
    assert record.shuffle is False
    assert record.skipped is False
    assert record.offline is False
    assert record.incognito_mode is False
    assert record.extracted_track_id == "3JcC0WHJMBWNpzDR1Npfj7"


def test_extracted_track_id_valid():
    """Test track ID extraction from a valid spotify:track: URI."""
    record = SpotifyExportRecord(
        **{
            "ts": "2026-01-01T00:00:00Z",
            "master_metadata_album_artist_name": "A",
            "master_metadata_track_name": "B",
            "ms_played": 1000,
            "spotify_track_uri": "spotify:track:abc123",
        }
    )
    assert record.extracted_track_id == "abc123"


def test_extracted_track_id_episode():
    """Episode URIs should return None for track ID."""
    record = SpotifyExportRecord(
        **{
            "ts": "2026-01-01T00:00:00Z",
            "master_metadata_album_artist_name": None,
            "master_metadata_track_name": None,
            "ms_played": 1000,
            "spotify_track_uri": "spotify:episode:xyz789",
        }
    )
    assert record.extracted_track_id is None


def test_extracted_track_id_null_uri():
    """Missing URI should return None for track ID."""
    record = SpotifyExportRecord(
        **{
            "ts": "2026-01-01T00:00:00Z",
            "master_metadata_album_artist_name": "A",
            "master_metadata_track_name": "B",
            "ms_played": 1000,
        }
    )
    assert record.extracted_track_id is None


def test_standard_export_new_fields_default_none():
    """Standard format records should have all extended fields as None."""
    record = SpotifyExportRecord(
        **{
            "endTime": "2026-03-10 07:57",
            "artistName": "Daft Punk",
            "trackName": "Something",
            "msPlayed": 200000,
        }
    )
    assert record.platform is None
    assert record.shuffle is None
    assert record.conn_country is None
    assert record.extracted_track_id is None


def test_spotify_export_missing_required_fields():
    """Test that missing required fields raise an error."""
    bad_data = {
        "artistName": "Daft Punk"
        # Missing endTime and msPlayed
    }
    with pytest.raises(ValidationError):
        SpotifyExportRecord(**bad_data)
