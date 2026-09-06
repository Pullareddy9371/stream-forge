import json
from datetime import datetime, timedelta, timezone

from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    collect_window,
)

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
        print(
            f"[INVALID MESSAGE] Skipping non-JSON message: {value}"
        )
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


def get_event_timestamp(event):
    """Convert the telemetry timestamp into a UTC datetime."""

    timestamp = event.get("timestamp")

    if not timestamp:
        return datetime.now(timezone.utc)

    parsed_timestamp = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )

    if parsed_timestamp.tzinfo is None:
        parsed_timestamp = parsed_timestamp.replace(
            tzinfo=timezone.utc
        )

    return parsed_timestamp.astimezone(timezone.utc)


def format_window_output(item):
    """Format collected window data for readable output."""

    truck_id, (window_id, events) = item

    return {
        "truck_id": truck_id,
        "window_id": window_id,
        "event_count": len(events),
        "events": events,
    }
def calculate_window_average(item):
    """Calculate average temperature for a truck within a window."""

    truck_id, (window_id, events) = item

    temperatures = [
        event["temperature"]
        for event in events
        if event.get("temperature") is not None
    ]

    average_temperature = (
        sum(temperatures) / len(temperatures)
        if temperatures
        else 0.0
    )

    return {
        "truck_id": truck_id,
        "window_id": window_id,
        "event_count": len(events),
        "average_temperature": round(average_temperature, 2),
    }


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

    keyed_stream = op.key_on(
        "key-by-truck",
        mapped_stream,
        lambda event: event["truck_id"],
    )

    event_clock = EventClock(
        ts_getter=get_event_timestamp,
        wait_for_system_duration=timedelta(seconds=0),
    )

    windower = TumblingWindower(
        length=timedelta(minutes=5),
        align_to=datetime(
            2026,
            1,
            1,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        ),
    )

    windowed_stream = collect_window(
        "collect-5-minute-window",
        keyed_stream,
        event_clock,
        windower,
    )

    aggregated_stream = op.map(
    "calculate-window-average",
    windowed_stream.down,
    calculate_window_average,
    )

    op.inspect(
    "print-windowed-events",
    aggregated_stream,

    )

    
    return flow


flow = build_flow()