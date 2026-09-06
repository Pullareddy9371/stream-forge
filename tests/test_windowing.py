from datetime import datetime, timezone

from backend.stream_processor.main import get_event_timestamp


def test_get_event_timestamp():
    event = {
        "truck_id": "TRUCK001",
        "timestamp": "2026-09-06T10:15:30+00:00",
    }

    result = get_event_timestamp(event)

    assert result == datetime(
        2026,
        9,
        6,
        10,
        15,
        30,
        tzinfo=timezone.utc,
    )


def test_get_event_timestamp_with_z():
    event = {
        "truck_id": "TRUCK002",
        "timestamp": "2026-09-06T10:20:00Z",
    }

    result = get_event_timestamp(event)

    assert result == datetime(
        2026,
        9,
        6,
        10,
        20,
        0,
        tzinfo=timezone.utc,
    )


def test_get_event_timestamp_missing():
    event = {
        "truck_id": "TRUCK003",
    }

    result = get_event_timestamp(event)

    assert result.tzinfo == timezone.utc