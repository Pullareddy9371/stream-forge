import os


RECOVERY_DIR = os.getenv(
    "BYTEWAX_RECOVERY_DIR",
    "backend/stream_processor/recovery",
)

RECOVERY_PARTITIONS = int(
    os.getenv(
        "BYTEWAX_RECOVERY_PARTITIONS",
        "3",
    )
)

RECOVERY_INTERVAL_SECONDS = int(
    os.getenv(
        "BYTEWAX_RECOVERY_INTERVAL_SECONDS",
        "30",
    )
)