from unittest.mock import patch

from backend.stream_processor.main import (
    instrumented_update_temperature_state,
    parse_event,
)


def test_parse_invalid_event_increments_invalid_metric():
    event = type("Event", (), {"value": b"invalid-json"})()

    with patch(
        "backend.stream_processor.main.INVALID_EVENTS.inc"
    ) as mock_increment:
        result = parse_event(event)

    assert result is None
    mock_increment.assert_called_once()


def test_parse_valid_event_does_not_increment_invalid_metric():
    event = type(
        "Event",
        (),
        {
            "value": b'{"truck_id": "TRUCK-00001", "temperature": 30.5}'
        },
    )()

    with patch(
        "backend.stream_processor.main.INVALID_EVENTS.inc"
    ) as mock_increment:
        result = parse_event(event)

    assert result["truck_id"] == "TRUCK-00001"
    mock_increment.assert_not_called()


def test_successful_state_update_increments_processed_metric():
    keyed_event = (
        "TRUCK-00001",
        {
            "temperature": 30.5,
        },
    )

    with patch(
        "backend.stream_processor.main.update_temperature_state",
        side_effect=lambda state, value: (
            "TRUCK-00001",
            {
                "temperature": 30.5,
                "average_temperature": 30.5,
                "count": 1,
            },
        ),
    ), patch(
        "backend.stream_processor.main.EVENTS_PROCESSED.inc"
    ) as mock_processed:

        result = instrumented_update_temperature_state(None, keyed_event)

    assert result[0] == "TRUCK-00001"
    mock_processed.assert_called_once()


def test_processing_error_increments_error_metric():
    keyed_event = (
        "TRUCK-00001",
        {
            "temperature": 30.5,
        },
    )

    with patch(
        "backend.stream_processor.main.update_temperature_state",
        side_effect=RuntimeError("test processing error"),
    ), patch(
        "backend.stream_processor.main.PROCESSING_ERRORS.inc"
    ) as mock_errors:

        try:
            instrumented_update_temperature_state(None, keyed_event)
        except RuntimeError:
            pass

    mock_errors.assert_called_once()


def test_processing_latency_is_recorded():
    keyed_event = (
        "TRUCK-00001",
        {
            "temperature": 30.5,
        },
    )

    with patch(
        "backend.stream_processor.main.update_temperature_state",
        return_value=(
            "TRUCK-00001",
            {
                "temperature": 30.5,
                "average_temperature": 30.5,
                "count": 1,
            },
        ),
    ), patch(
        "backend.stream_processor.main.PROCESSING_LATENCY.observe"
    ) as mock_latency:

        instrumented_update_temperature_state(None, keyed_event)

    mock_latency.assert_called_once()