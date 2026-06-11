"""Загрузка и индексация исходных JSONL-событий."""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


logger = logging.getLogger("generator")


@dataclass
class EventDictionary:
    """Базовый словарь событий из JSONL файлов."""

    browser_events: list[dict[str, Any]]
    location_events: list[dict[str, Any]]
    device_events: list[dict[str, Any]]
    geo_events: list[dict[str, Any]]
    browser_by_click_id: dict[str, list[dict]] = field(default_factory=dict)
    location_by_event_id: dict[str, dict] = field(default_factory=dict)
    device_by_click_id: dict[str, dict] = field(default_factory=dict)
    geo_by_click_id: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self):
        for browser in self.browser_events:
            self.browser_by_click_id.setdefault(browser["click_id"], []).append(browser)
        for loc in self.location_events:
            self.location_by_event_id[loc["event_id"]] = loc
        for dev in self.device_events:
            self.device_by_click_id[dev["click_id"]] = dev
        for geo in self.geo_events:
            self.geo_by_click_id[geo["click_id"]] = geo

    @classmethod
    def load(cls, data_dir: Path) -> "EventDictionary":
        """Загружает события из JSONL файлов."""
        logger.info(f"Loading event dictionary from {data_dir}")

        def load_jsonl(filename: str) -> list[dict]:
            path = data_dir / filename
            events = []
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
            logger.info(f"  Loaded {len(events)} events from {filename}")
            return events

        browser_events = load_jsonl("browser_events.jsonl")
        location_events = load_jsonl("location_events.jsonl")
        device_events = load_jsonl("device_events.jsonl")
        geo_events = load_jsonl("geo_events.jsonl")

        if not browser_events:
            raise ValueError("browser_events.jsonl is empty or missing")

        return cls(
            browser_events=browser_events,
            location_events=location_events,
            device_events=device_events,
            geo_events=geo_events,
        )
