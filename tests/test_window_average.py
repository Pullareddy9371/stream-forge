from backend.stream_processor.main import calculate_window_average


def test_calculate_window_average():
    item = (
        "TRUCK001",
        (
            100,
            [
                {"temperature": 20.0},
                {"temperature": 30.0},
                {"temperature": 40.0},
            ],
        ),
    )

    result = calculate_window_average(item)

    assert result["truck_id"] == "TRUCK001"
    assert result["window_id"] == 100
    assert result["event_count"] == 3
    assert result["average_temperature"] == 30.0


def test_calculate_window_average_ignores_missing_temperature():
    item = (
        "TRUCK002",
        (
            101,
            [
                {"temperature": 20.0},
                {"temperature": None},
                {"temperature": 40.0},
            ],
        ),
    )

    result = calculate_window_average(item)

    assert result["average_temperature"] == 30.0
    assert result["event_count"] == 3


def test_calculate_window_average_empty_events():
    item = (
        "TRUCK003",
        (
            102,
            [],
        ),
    )

    result = calculate_window_average(item)

    assert result["event_count"] == 0
    assert result["average_temperature"] == 0.0