from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
)


def print_event(step_id, event):
    """Print events received from Kafka."""
    print(f"[KAFKA EVENT] {event}")


def build_flow():
    """Build the StreamForge Kafka consumption flow."""

    flow = Dataflow("streamforge")

    source = KafkaSource(
        brokers=[KAFKA_BOOTSTRAP_SERVERS],
        topics=[KAFKA_INPUT_TOPIC],
        tail=True,
        starting_offset=-2,
    )

    stream = op.input(
        "kafka-input",
        flow,
        source,
    )

    op.inspect(
        "print-events",
        stream,
        print_event,
    )

    return flow


flow = build_flow()