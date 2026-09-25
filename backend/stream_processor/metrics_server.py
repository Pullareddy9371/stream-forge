from prometheus_client import start_http_server

from .metrics import (
    ACTIVE_WORKERS,
    EVENTS_PROCESSED,
    INVALID_EVENTS,
    PROCESSING_ERRORS,
    PROCESSING_LATENCY,
)


def start_metrics_server(port=8000):
    """Start the Prometheus metrics HTTP server."""
    start_http_server(port)
    print(f"Prometheus metrics server started on port {port}")


if __name__ == "__main__":
    start_metrics_server()

    print("Prometheus metrics available at http://localhost:8000/metrics")
    print("Press Ctrl+C to stop the server.")

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("\nPrometheus metrics server stopped.")