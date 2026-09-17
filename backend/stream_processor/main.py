import json
from datetime import datetime, timezone
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
def get_event_timestamp(event):
    """Parse the telemetry timestamp into a timezone-aware datetime."""
    timestamp = event.get("timestamp")

    if timestamp is None:
        return datetime.now(timezone.utc)

    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1] + "+00:00"

    parsed = datetime.fromisoformat(timestamp)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def temperature_filter(event):
    """Allow only telemetry events with temperature greater than 0."""
    return event is not None and event.get("temperature", 0) > 0


def map_telemetry(event):
    """Add temperature status while preserving existing telemetry fields."""
    if event is None:
        return None

    result = event.copy()
    temperature = result.get("temperature")

    if temperature is not None and temperature >= 40:
        result["temperature_status"] = "high"
    else:
        result["temperature_status"] = "normal"

    return result


def calculate_window_average(item):
    """Calculate average temperature for a windowed telemetry item."""
    truck_id, window_data = item
    window_id, events = window_data

    temperatures = [
        event["temperature"]
        for event in events
        if event.get("temperature") is not None
    ]

    if temperatures:
        average_temperature = sum(temperatures) / len(temperatures)
    else:
        average_temperature = 0.0

    return {
        "truck_id": truck_id,
        "window_id": window_id,
        "event_count": len(events),
        "average_temperature": average_temperature,
    }

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