#!/usr/bin/env python3
"""
Совместимый фасад и точка входа генератора.

Основной код разнесён по модулям рядом с этим файлом. Старые импорты вида
`from generator import Config` сохраняются для тестов и внешних запусков.
"""

import logging
import sys
from pathlib import Path


SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from clickstream_generator.config import Config
from clickstream_generator.dictionary import EventDictionary
from clickstream_generator.generation import EXPECTED_VISIT_EVENTS, EventGenerator
from clickstream_generator.intensity import calculate_events_count, hour_factor
from clickstream_generator.kafka_io import (
    BatchRecord,
    KafkaBatchHistory,
    KafkaPublisher,
    KafkaStateManager,
    KafkaStartupHistoryManifest,
    _import_kafka,
    _with_retry,
    ensure_topics,
)
from clickstream_generator.metrics import (
    METRICS_ERRORS_TOTAL,
    METRICS_EVENTS_TOTAL,
    METRICS_LAST_SUCCESS,
    METRICS_TICK_DURATION,
)
from clickstream_generator.runtime import TickStreamGenerator, generate_tick_batch
from clickstream_generator.service import GeneratorService, main
from clickstream_generator.state import GeneratorState, _nested_list_to_tuple


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

__all__ = [
    "BatchRecord",
    "Config",
    "EventDictionary",
    "EventGenerator",
    "EXPECTED_VISIT_EVENTS",
    "GeneratorService",
    "GeneratorState",
    "KafkaBatchHistory",
    "KafkaPublisher",
    "KafkaStateManager",
    "KafkaStartupHistoryManifest",
    "METRICS_ERRORS_TOTAL",
    "METRICS_EVENTS_TOTAL",
    "METRICS_LAST_SUCCESS",
    "METRICS_TICK_DURATION",
    "TickStreamGenerator",
    "_import_kafka",
    "_nested_list_to_tuple",
    "_with_retry",
    "calculate_events_count",
    "ensure_topics",
    "generate_tick_batch",
    "hour_factor",
    "main",
]


if __name__ == "__main__":
    main()
