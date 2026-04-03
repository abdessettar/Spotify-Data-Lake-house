import sys
from src.config import settings
from azure.core.exceptions import ResourceExistsError


def verify():
    print(f"Environment: {settings.ENVIRONMENT}")

    # 1. Get Client
    blob_service = settings.get_blob_service_client()

    # 2. Try to create the container (Bronze/Silver/Gold home)
    container_client = blob_service.get_container_client(settings.DATA_CONTAINER)

    try:
        container_client.create_container()
        print(f"Created container '{settings.DATA_CONTAINER}'.")
    except ResourceExistsError:
        print(f"Container '{settings.DATA_CONTAINER}' already exists.")
    except Exception as e:
        print(f"Failed to connect to Storage: {e}")
        sys.exit(1)

    print("Configuration System is a GO!")


if __name__ == "__main__":
    verify()
