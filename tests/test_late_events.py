from datetime import datetime, timedelta, timezone

from backend.stream_processor.main import get_event_timestamp


def test_out_of_order_timestamp_is_parsed():
    event = {
        "truck_id": "TRUCK001",
        "timestamp": "2026-09-06T10:00:00+00:00",
    }

    result = get_event_timestamp(event)

    assert result == datetime(
        2026,
        9,
        6,
        10,
        0,
        0,
        tzinfo=timezone.utc,
    )


def test_late_event_timestamp_is_older():
    current_event_time = datetime(
        2026,
        9,
        6,
        10,
        5,
        0,
        tzinfo=timezone.utc,
    )

    late_event_time = datetime(
        2026,
        9,
        6,
        9,
        59,
        0,
        tzinfo=timezone.utc,
    )

    assert late_event_time < current_event_time


def test_lateness_tolerance():
    tolerance = timedelta(seconds=30)

    current_event_time = datetime(
        2026,
        9,
        6,
        10,
        0,
        30,
        tzinfo=timezone.utc,
    )

    late_event_time = datetime(
        2026,
        9,
        6,
        10,
        0,
        0,
        tzinfo=timezone.utc,
    )

    difference = current_event_time - late_event_time

    assert difference <= tolerance