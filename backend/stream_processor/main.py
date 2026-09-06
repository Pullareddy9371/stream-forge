import json

from bytewax import operators as op
from bytewax.dataflow import Dataflow

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    KAFKA_GROUP_ID,
)


flow = Dataflow("streamforge")

# Day 9:
# Stream processing foundation will be implemented here.
#
# Day 10:
# Kafka Consume stage
#
# Day 11:
# Temperature Filter stage
#
# Day 12:
# Map stage
#
# Day 13:
# Five-minute event-time window
#
# Day 14:
# Per-truck temperature aggregation


if __name__ == "__main__":
    print("StreamForge Stream Processor")
    print("--------------------------------")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Input topic: {KAFKA_INPUT_TOPIC}")
    print(f"Consumer group: {KAFKA_GROUP_ID}")
    print("--------------------------------")
    print("Stream processing application initialized.")