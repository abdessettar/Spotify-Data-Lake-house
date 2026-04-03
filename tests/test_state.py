from unittest.mock import MagicMock, patch
from src.core.state import StateManager
from azure.core.exceptions import ResourceNotFoundError


@patch("src.core.state.settings")
def test_get_cursor_file_exists(mock_settings):
    """Test reading an existing cursor."""
    # Mock chain: settings.get_blob_service_client() → BlobServiceClient → get_blob_client() → BlobClient
    # This simulates the full Azure SDK call path without a real connection.
    mock_blob_service = MagicMock()
    mock_blob_client = MagicMock()
    mock_settings.get_blob_service_client.return_value = mock_blob_service
    mock_blob_service.get_blob_client.return_value = mock_blob_client

    # Simulate downloading a JSON file
    mock_download = MagicMock()
    mock_download.readall.return_value = b'{"last_run_timestamp": "2023-10-01T12:00:00Z", "last_played_at_unix_ms": 1698000000000}'
    mock_blob_client.download_blob.return_value = mock_download

    manager = StateManager()
    cursor = manager.get_cursor()

    assert cursor.last_played_at_unix_ms == 780415200
    assert cursor.last_run_timestamp.year == 1994


@patch("src.core.state.settings")
def test_get_cursor_file_not_found(mock_settings):
    """Test fallback when no cursor exists (First run scenario)."""
    mock_blob_service = MagicMock()
    mock_blob_client = MagicMock()
    mock_settings.get_blob_service_client.return_value = mock_blob_service
    mock_blob_service.get_blob_client.return_value = mock_blob_client

    # ResourceNotFoundError simulates a first-run scenario where no cursor blob exists yet.
    # StateManager should catch this and return a safe default cursor instead of crashing.
    mock_blob_client.download_blob.side_effect = ResourceNotFoundError("Blob not found")

    manager = StateManager()
    cursor = manager.get_cursor()

    # Should default to 0 to fetch all history
    assert cursor.last_played_at_unix_ms == 0
    assert cursor.last_run_timestamp.year == 2017
