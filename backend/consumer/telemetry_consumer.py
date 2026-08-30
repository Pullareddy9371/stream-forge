import json

from confluent_kafka import Consumer, KafkaException
from validator import validate_telemetry
from processor import process_telemetry


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
    """Decode and validate a Kafka message."""

    try:
        data = json.loads(
            message.value().decode("utf-8")
        )

    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        print(f"[INVALID] Invalid JSON | error={error}")
        return

    is_valid, error = validate_telemetry(data)

    if not is_valid:
        print(
            f"[INVALID] "
            f"truck_id={data.get('truck_id', 'UNKNOWN')} | "
            f"reason={error}"
        )
        return

    result = process_telemetry(data)

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