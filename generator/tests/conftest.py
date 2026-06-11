"""
Pytest fixtures для тестирования генератора.
"""

import sys
from pathlib import Path

GENERATOR_DIR = Path(__file__).parent.parent

# Добавляем фасад generator.py и src-пакет в путь
sys.path.insert(0, str(GENERATOR_DIR))
sys.path.insert(0, str(GENERATOR_DIR / "src"))

import pytest
from generator import Config, EventDictionary


@pytest.fixture
def data_dir():
    """Путь к директории с тестовыми данными."""
    return Path(__file__).parent.parent.parent / "data"


@pytest.fixture
def event_dictionary(data_dir):
    """Загруженный словарь событий."""
    return EventDictionary.load(data_dir)


@pytest.fixture
def base_config(data_dir):
    """Базовая конфигурация для тестов."""
    return Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=5,
        lambda_base_per_min=200,
        jitter_pct=20,
        min_events_per_tick=5,
        max_events_per_tick=50,
        data_dir=data_dir,
        seed=42,
        enabled=True,
        metrics_port=9109,
        state_enabled=True,
        state_reset=False,
    )


@pytest.fixture
def config_no_jitter(base_config):
    """Конфигурация без jitter."""
    from dataclasses import replace
    return replace(base_config, jitter_pct=0)


@pytest.fixture
def empty_temp_dir(tmp_path):
    """Временная директория с пустыми JSONL файлами."""
    for fname in ["browser_events.jsonl", "location_events.jsonl",
                  "device_events.jsonl", "geo_events.jsonl"]:
        (tmp_path / fname).touch()
    return tmp_path
