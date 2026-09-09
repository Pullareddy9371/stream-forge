import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_INPUT_TOPIC = os.getenv(
    "KAFKA_INPUT_TOPIC",
    "truck-telemetry",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "streamforge-stream-processor",
)