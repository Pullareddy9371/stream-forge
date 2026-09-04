import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "truck-telemetry",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "streamforge-telemetry-consumer",
)

AUTO_OFFSET_RESET = os.getenv(
    "KAFKA_AUTO_OFFSET_RESET",
    "earliest",
)