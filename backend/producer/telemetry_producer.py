import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    TRUCK_COUNT,
    SEND_INTERVAL_SECONDS,
)


def delivery_report(err, message):
    """Called by Kafka after a message is delivered or fails."""

    if err is not None:
        print(
            f"Delivery failed for truck event: {err}"
        )
        return

    print(
        f"Delivered truck event | "
        f"truck_id={message.key().decode()} | "
        f"partition={message.partition()} | "
        f"offset={message.offset()}"
    )


def generate_telemetry(truck_id):
    """Generate one simulated truck telemetry event."""

    return {
        "truck_id": truck_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(
            random.uniform(12.5, 19.5), 6
        ),
        "longitude": round(
            random.uniform(72.5, 80.5), 6
        ),
        "speed_kmph": round(
            random.uniform(20, 90), 2
        ),
        "temperature": round(
            random.uniform(20, 40), 2
        ),
        "fuel_level": round(
            random.uniform(10, 100), 2
        ),
    }


def create_producer():
    """Create and return a Kafka producer."""

    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "streamforge-telemetry-producer",
    }

    return Producer(producer_config)


def run():
    """Generate and publish truck telemetry continuously."""

    producer = create_producer()

    print("StreamForge Telemetry Producer")
    print("--------------------------------")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Trucks: {TRUCK_COUNT}")
    print(
        f"Interval: {SEND_INTERVAL_SECONDS} seconds"
    )
    print("--------------------------------")

    try:
        while True:

            events_sent = 0

            for truck_number in range(1, TRUCK_COUNT + 1):

                truck_id = (
                    f"TRUCK-{truck_number:05d}"
                )

                telemetry = generate_telemetry(
                    truck_id
                )

                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=truck_id.encode("utf-8"),
                    value=json.dumps(
                        telemetry
                    ).encode("utf-8"),
                    callback=delivery_report,
                )

                events_sent += 1

                # Process delivery callbacks.
                producer.poll(0)

            producer.flush()

            print(
                f"\nBatch completed: "
                f"{events_sent} telemetry events sent."
            )

            print(
                f"Waiting {SEND_INTERVAL_SECONDS} seconds..."
            )

            time.sleep(SEND_INTERVAL_SECONDS)

    except KeyboardInterrupt:

        print(
            "\nStopping StreamForge producer..."
        )

    finally:

        producer.flush()

        print("Producer stopped.")


if __name__ == "__main__":
    run()