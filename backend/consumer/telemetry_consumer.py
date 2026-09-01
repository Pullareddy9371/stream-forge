import json

from confluent_kafka import Consumer, KafkaException
from validator import validate_telemetry
from processor import process_telemetry
from logger_config import setup_logger

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


def process_message(message):
    try:
        raw_value = message.value()

        data = json.loads(raw_value)

    except json.JSONDecodeError as error:
        logger.error(
            "Invalid JSON received | error=%s | message=%s",
            error,
            raw_value,
        )
        return
    is_valid, error = validate_telemetry(data)
    if not is_valid:
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
        logger.exception(
            "Telemetry processing failed | truck_id=%s | error=%s",
            data.get("truck_id", "UNKNOWN"),
            error,
        )
        return

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

    try:
        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            process_message(message)

    except KeyboardInterrupt:
        print("\nStopping StreamForge consumer...")

    finally:
        consumer.close()
        print("Consumer stopped.")


if __name__ == "__main__":
    run()