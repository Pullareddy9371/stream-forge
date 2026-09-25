import os
from pathlib import Path

from rocksdict import Rdict


def get_state_db_path() -> str:
    """
    Return a process-specific RocksDB path.

    Process 0 keeps using the existing Day 19 database so
    previously persisted state remains available.
    """

    process_id = os.getenv("BYTEWAX_PROCESS_ID", "0")

    if process_id == "0":
        return "backend/stream_processor/state_db"

    return f"backend/stream_processor/state_db_process_{process_id}"


class RocksDBStateStore:
    """Persistent key-value state store backed by RocksDB."""

    def __init__(self, db_path: str | None = None):
        if db_path is None:
            db_path = get_state_db_path()

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = Rdict(str(self.db_path))

    def put(self, key: str, value: dict) -> None:
        """Store state for a key."""
        self.db[key] = value

    def get(self, key: str):
        """Retrieve state for a key."""
        return self.db.get(key)

    def delete(self, key: str) -> None:
        """Delete state for a key."""
        if key in self.db:
            del self.db[key]

    def close(self) -> None:
        """Close the RocksDB database."""
        self.db.close()