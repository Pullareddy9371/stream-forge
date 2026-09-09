import json

from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource
from bytewax.dataflow import Dataflow

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
)
from .state import update_temperature_state


def parse_event(event):
    """Convert Kafka event into a Python dictionary."""
    try:
        return json.loads(event.value)
    except (json.JSONDecodeError, TypeError):
        return None


def print_result(step_id, event):
    """Print stateful temperature results."""
    print(
        f"[STATE] "
        f"truck_id={event['truck_id']} | "
        f"temperature={event['temperature']} | "
        f"average={event['average_temperature']} | "
        f"count={event['count']}"
    )


def build_flow():
    """Build the StreamForge stateful processing flow."""

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

    events = op.map(
        "parse-event",
        stream,
        parse_event,
    )

    valid_events = op.filter(
        "valid-events",
        events,
        lambda event: event is not None and "truck_id" in event,
    )

    keyed_events = op.key_on(
        "truck-key",
        valid_events,
        lambda event: event["truck_id"],
    )

    stateful_events = op.stateful_map(
        "truck-temperature-state",
        keyed_events,
        update_temperature_state,
    )

    results = op.map(
        "state-result",
        stateful_events,
        lambda item: item[1],
    )

    op.inspect(
        "print-state",
        results,
        print_result,
    )

    return flow


flow = build_flow()