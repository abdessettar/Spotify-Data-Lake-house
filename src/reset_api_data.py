from src.config import settings
from azure.core.exceptions import ResourceNotFoundError


def run():
    blob_service = settings.get_blob_service_client()
    container_client = blob_service.get_container_client(settings.DATA_CONTAINER)

    print("1. Deleting old Bronze API data...")
    blobs = container_client.list_blobs(name_starts_with="bronze/spotify_api/")
    count = 0
    for blob in blobs:
        container_client.delete_blob(blob.name)
        count += 1
    print(f"Deleted {count} old JSON files.")

    print("2. Resetting pipeline state (cursor)...")
    try:
        container_client.delete_blob("system/state/cursor.json")
        print("Deleted cursor.json. Pipeline will start fresh.")
    except ResourceNotFoundError:
        print("No cursor found to delete.")


if __name__ == "__main__":
    run()
