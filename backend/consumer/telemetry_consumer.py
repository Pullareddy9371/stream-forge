import json

from confluent_kafka import Consumer, KafkaException
from validator import validate_telemetry
from processor import process_telemetry
from logger_config import setup_logger
class ConsumerStats:
    def __init__(self):
        self.processed = 0
        self.invalid_json = 0
        self.validation_failed = 0
        self.errors = 0

    def print_summary(self):
        total = (
            self.processed
            + self.invalid_json
            + self.validation_failed
        )

        print("\n========== Consumer Summary ==========")
        print(f"Total messages:      {total}")
        print(f"Processed:           {self.processed}")
        print(f"Invalid JSON:        {self.invalid_json}")
        print(f"Validation failed:   {self.validation_failed}")
        print(f"Processing errors:   {self.errors}")
        print("======================================")

logger = setup_logger()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "truck-telemetry"
KAFKA_GROUP_ID = "streamforge-telemetry-consumer"


def create_consumer():
    """Create and configure the Kafka consumer."""

    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    return Consumer(config)

def process_message(message, stats):
    try:
        raw_value = message.value()
        data = json.loads(raw_value)

    except json.JSONDecodeError as error:
        stats.invalid_json += 1

        logger.error(
            "Invalid JSON received | error=%s | message=%s",
            error,
            raw_value,
        )
        return

    is_valid, error = validate_telemetry(data)

    if not is_valid:
        stats.validation_failed += 1

        logger.warning(
            "Telemetry validation failed | truck_id=%s | reason=%s",
            data.get("truck_id", "UNKNOWN"),
            error,
        )

        print(
            f"[INVALID] "
            f"truck_id={data.get('truck_id', 'UNKNOWN')} | "
            f"reason={error}"
        )

        return

    try:
        result = process_telemetry(data)

    except Exception as error:
        stats.errors += 1

        logger.exception(
            "Telemetry processing failed | truck_id=%s | error=%s",
            data.get("truck_id", "UNKNOWN"),
            error,
        )
        return

    stats.processed += 1

    logger.info(
        "Telemetry processed | "
        "truck_id=%s | speed=%s | temperature=%s | fuel=%s | overall=%s",
        result["truck_id"],
        result["speed_status"],
        result["temperature_status"],
        result["fuel_status"],
        result["overall_status"],
    )

    print(
        f"[VALID] "
        f"truck_id={result['truck_id']} | "
        f"speed={result['speed_status']} | "
        f"temperature={result['temperature_status']} | "
        f"fuel={result['fuel_status']} | "
        f"overall={result['overall_status']}"
    )

def run():
    """Consume truck telemetry from Kafka."""

    consumer = create_consumer()

    consumer.subscribe([KAFKA_TOPIC])

    print("StreamForge Telemetry Consumer")
    print("--------------------------------")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Group: {KAFKA_GROUP_ID}")
    print("--------------------------------")

    stats = ConsumerStats()

    try:
        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            process_message(message, stats)

    except KeyboardInterrupt:
        print("\nStopping StreamForge consumer...")
    finally:
        consumer.close()
        print("Consumer stopped.")
        stats.print_summary()


if __name__ == "__main__":
    run()