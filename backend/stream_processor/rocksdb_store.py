from pathlib import Path

from rocksdict import Rdict


class RocksDBStateStore:
    """Persistent key-value state store backed by RocksDB."""

    def __init__(self, db_path: str = "backend/stream_processor/state_db"):
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