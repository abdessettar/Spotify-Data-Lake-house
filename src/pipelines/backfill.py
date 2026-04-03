import json
from pathlib import Path
from pydantic import ValidationError
from src.connectors.storage import DataLakeConnector
from src.domain.schemas import SpotifyExportRecord


class BackfillPipeline:
    def __init__(self):
        self.storage_connector = DataLakeConnector()

    def run(self, data_dir: str):
        print(f"--- Starting Historical Backfill from {data_dir} ---")

        export_path = Path(data_dir)
        if not export_path.exists() or not export_path.is_dir():
            print(f"Error: Directory '{data_dir}' does not exist.")
            return

        # Find all JSON files that look like streaming history:
        # Standard exports use "StreamingHistory_music_0.json" naming.
        # Extended exports use "Streaming_History_Audio_*.json" naming (with underscores).
        json_files = [
            f
            for f in export_path.glob("*.json")
            if "StreamingHistory" in f.name or "Streaming_History" in f.name
        ]

        if not json_files:
            print(f"No streaming history files found in {data_dir}.")
            print("Files must contain 'StreamingHistory' or 'endsong' in their name.")
            return

        total_records_processed = 0

        for file_path in json_files:
            print(f"Processing {file_path.name}...")

            with open(file_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)

            valid_records = []
            error_count = 0

            # Validate each record in the JSON array
            for item in raw_data:
                try:
                    record = SpotifyExportRecord(**item)
                    # Filter out podcasts: in Spotify exports, podcast episodes have null
                    # track_name or artist_name, while music tracks always have both populated.
                    if record.track_name and record.artist_name:
                        valid_records.append(record)
                except ValidationError:
                    print(
                        f"Skipping malformed record: {item} in file {file_path.name}."
                    )
                    error_count += 1

            total_records_processed += len(valid_records)
            print(
                f"  -> Validated {len(valid_records)} music tracks (Ignored {error_count} malformed/podcast records)."
            )

            # Read the raw bytes of the original file separately from the parsed JSON above.
            # We upload the RAW, unmodified source file to Bronze (not the validated records)
            # to preserve the original data exactly as Spotify provided it.
            with open(file_path, "rb") as bf:
                raw_bytes = bf.read()

            self.storage_connector.save_backfill_file(
                filename=file_path.name, content=raw_bytes
            )

        print(
            f"Backfill Complete! Uploaded {len(json_files)} files containing {total_records_processed} valid tracks."
        )
