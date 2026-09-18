from prometheus_client import Counter, Gauge, Histogram


EVENTS_PROCESSED = Counter(
    "streamforge_events_processed_total",
    "Total number of telemetry events processed.",
)

INVALID_EVENTS = Counter(
    "streamforge_invalid_events_total",
    "Total number of invalid telemetry events.",
)

PROCESSING_ERRORS = Counter(
    "streamforge_processing_errors_total",
    "Total number of telemetry processing errors.",
)

ACTIVE_WORKERS = Gauge(
    "streamforge_active_workers",
    "Number of active StreamForge workers.",
)

PROCESSING_LATENCY = Histogram(
    "streamforge_processing_latency_seconds",
    "Telemetry event processing latency in seconds.",
)