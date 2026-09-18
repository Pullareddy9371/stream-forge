from backend.stream_processor.metrics import (
    EVENTS_PROCESSED,
    INVALID_EVENTS,
    PROCESSING_ERRORS,
    ACTIVE_WORKERS,
    PROCESSING_LATENCY,
)


def test_events_processed_counter_exists():
    assert EVENTS_PROCESSED is not None


def test_invalid_events_counter_exists():
    assert INVALID_EVENTS is not None


def test_processing_errors_counter_exists():
    assert PROCESSING_ERRORS is not None


def test_active_workers_gauge_exists():
    assert ACTIVE_WORKERS is not None


def test_processing_latency_histogram_exists():
    assert PROCESSING_LATENCY is not None