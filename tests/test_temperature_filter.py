from backend.stream_processor.main import temperature_filter


def test_temperature_above_zero():
    event = {
        "truck_id": "TRUCK001",
        "temperature": 25.5,
        "timestamp": "2026-09-06T10:00:00"
    }

    assert temperature_filter(event) is True


def test_temperature_zero():
    event = {
        "truck_id": "TRUCK002",
        "temperature": 0,
        "timestamp": "2026-09-06T10:01:00"
    }

    assert temperature_filter(event) is False


def test_temperature_below_zero():
    event = {
        "truck_id": "TRUCK003",
        "temperature": -5.5,
        "timestamp": "2026-09-06T10:02:00"
    }

    assert temperature_filter(event) is False


def test_missing_temperature():
    event = {
        "truck_id": "TRUCK004",
        "timestamp": "2026-09-06T10:03:00"
    }

    assert temperature_filter(event) is False