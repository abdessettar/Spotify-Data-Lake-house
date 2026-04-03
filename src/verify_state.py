from datetime import datetime, timezone
from src.core.state import StateManager
from src.domain.schemas import PipelineCursor


def run():
    manager = StateManager()

    # 1. Read (Should be empty/default on first run)
    print("--- 1. Initial Read ---")
    cursor = manager.get_cursor()
    print(f"Current Cursor: {cursor.last_played_at_unix_ms}")

    # 2. Simulate a job run
    print("\n--- 2. Simulate Update ---")
    new_cursor = PipelineCursor(
        last_run_timestamp=datetime.now(timezone.utc),
        last_played_at_unix_ms=780415200,
    )
    manager.update_cursor(new_cursor)

    # 3. Read again (Should match what we wrote)
    print("\n--- 3. Verification Read ---")
    updated_cursor = manager.get_cursor()

    if updated_cursor.last_played_at_unix_ms == 780415200:
        print("SUCCESS: State persistence is working.")
    else:
        print("FAILURE: State mismatch.")


if __name__ == "__main__":
    run()
