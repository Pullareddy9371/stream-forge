from benchmarks.throughput_benchmark import process_event


def test_process_event_normal_temperature():
    event = {
        "truck_id": "TRUCK001",
        "temperature": 25.5,
    }

    result = process_event(event)

    assert result is not None
    assert result["temperature_status"] == "normal"


def test_process_event_high_temperature():
    event = {
        "truck_id": "TRUCK002",
        "temperature": 45.0,
    }

    result = process_event(event)

    assert result is not None
    assert result["temperature_status"] == "high"


def test_process_event_invalid_temperature():
    event = {
        "truck_id": "TRUCK003",
        "temperature": -5.0,
    }

    result = process_event(event)

    assert result is None