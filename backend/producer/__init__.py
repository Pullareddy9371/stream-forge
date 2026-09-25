import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "truck-telemetry"
)

TRUCK_COUNT = int(
    os.getenv("TRUCK_COUNT", "100")
)

SEND_INTERVAL_SECONDS = int(
    os.getenv("SEND_INTERVAL_SECONDS", "10")
)