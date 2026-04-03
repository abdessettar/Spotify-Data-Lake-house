from datetime import datetime, timedelta, timezone
from src.connectors.spotify import SpotifyClient


def run():
    print("--- Verifying Spotify Connector ---")
    client = SpotifyClient()

    # Calculate a timestamp for 3 days ago to ensure we get some data
    three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
    after_timestamp_ms = int(three_days_ago.timestamp() * 1000)

    print(f"Fetching tracks played after: {three_days_ago.isoformat()}")

    try:
        response = client.get_recently_played(
            after_timestamp_unix_ms=after_timestamp_ms
        )

        if response.items:
            print(f"SUCCESS: Fetched {len(response.items)} tracks.")
            first_track = response.items[0].track
            print(
                f"  -> Most recent track: '{first_track.name}' by {first_track.artists[0].name}"
            )
        else:
            print(
                "SUCCESS: Connection successful, but no new tracks found in the last 3 days."
            )

    except Exception as e:
        print(f"FAILURE: An error occurred during verification: {e}")


if __name__ == "__main__":
    run()
