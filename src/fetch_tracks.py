import json
import sys
from pathlib import Path

from src.connectors.spotify import SpotifyClient


def fetch_tracks_by_ids(
    track_ids: list[str], output_path: str = "tracks_output.json"
) -> dict:
    """
    Fetches full metadata for each track ID from the Spotify API and saves the result as JSON.

    Args:
        track_ids:   List of Spotify track IDs (e.g. ["5MmZraBvIW6liI1iET3pdK", ...]).
        output_path: File path where the JSON output will be written.

    Returns:
        A dict mapping each track ID to its API response (or None if the track was not found).
    """
    client = SpotifyClient()
    results: dict = {}

    for track_id in track_ids:
        print(f"Fetching track {track_id}...")
        data = client.get_track_by_id(track_id)
        results[track_id] = data
        if data is None:
            print(f"  Track {track_id} not found on Spotify.")

    Path(output_path).write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\nSaved {len(results)} track(s) to {output_path}")

    return results


if __name__ == "__main__":
    ids = sys.argv[1:] or [
        "5MmZraBvIW6liI1iET3pdK"
    ]
    fetch_tracks_by_ids(ids)
