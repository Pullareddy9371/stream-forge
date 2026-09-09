from backend.stream_processor.main import map_telemetry


def test_map_normal_temperature():
    event = {
        "truck_id": "TRUCK001",
        "temperature": 25.5,
    }

    result = map_telemetry(event)

    assert result["truck_id"] == "TRUCK001"
    assert result["temperature"] == 25.5
    assert result["temperature_status"] == "normal"


def test_map_high_temperature():
    event = {
        "truck_id": "TRUCK002",
        "temperature": 45.0,
    }

    result = map_telemetry(event)

    assert result["temperature_status"] == "high"


def test_map_preserves_existing_fields():
    event = {
        "truck_id": "TRUCK003",
        "temperature": 30.0,
        "speed": 65.0,
    }

    result = map_telemetry(event)

    assert result["truck_id"] == "TRUCK003"
    assert result["temperature"] == 30.0
    assert result["speed"] == 65.0