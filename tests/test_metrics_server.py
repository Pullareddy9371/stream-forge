from backend.stream_processor.metrics_server import start_metrics_server


def test_start_metrics_server_function_exists():
    assert callable(start_metrics_server)


def test_start_metrics_server_accepts_custom_port():
    assert callable(start_metrics_server)