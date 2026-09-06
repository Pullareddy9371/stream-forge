import json

from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource

from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
)


def print_event(step_id, event):
    """Print processed telemetry events."""
    print(f"[PROCESSED EVENT] {event}")


def decode_kafka_message(message):
    """Decode a KafkaSourceMessage into a telemetry dictionary."""

    value = message.value

    if value is None:
        return None

    if isinstance(value, bytes):
        value = value.decode("utf-8")

    try:
        return json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError):
        print(f"[INVALID MESSAGE] Skipping non-JSON message: {value}")
        return None


def temperature_filter(event):
    """Allow only telemetry events with temperature greater than 0."""

    if event is None:
        return False

    temperature = event.get("temperature")

    if temperature is None:
        return False

    return temperature > 0


def map_telemetry(event):
    """Transform valid telemetry events into an enriched event."""

    transformed_event = dict(event)

    temperature = transformed_event.get("temperature")

    if temperature is not None:
        transformed_event["temperature_status"] = (
            "normal" if temperature <= 40 else "high"
        )

    return transformed_event


def build_flow():
    """Build the StreamForge Kafka processing flow."""

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

    decoded_stream = op.map(
        "decode-json",
        stream,
        decode_kafka_message,
    )

    filtered_stream = op.filter(
        "temperature-filter",
        decoded_stream,
        temperature_filter,
    )

    mapped_stream = op.map(
        "telemetry-map",
        filtered_stream,
        map_telemetry,
    )

    op.inspect(
        "print-events",
        mapped_stream,
        print_event,
    )

    return flow


flow = build_flow()