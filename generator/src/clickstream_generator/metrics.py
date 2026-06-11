"""Prometheus-метрики генератора."""

from prometheus_client import Counter, Gauge, Histogram


METRICS_EVENTS_TOTAL = Counter(
    "generator_events_total",
    "Total number of events sent to Kafka",
    ["topic"],
)
METRICS_ERRORS_TOTAL = Counter(
    "generator_publish_errors_total",
    "Total number of publish errors",
    ["topic"],
)
METRICS_TICK_DURATION = Histogram(
    "generator_tick_duration_seconds",
    "Duration of generator tick in seconds",
)
METRICS_LAST_SUCCESS = Gauge(
    "generator_last_success_timestamp",
    "Unix timestamp of last successful tick",
)
