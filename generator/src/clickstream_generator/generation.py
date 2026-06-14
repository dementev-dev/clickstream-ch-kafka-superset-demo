"""Чистая генеративная модель визита."""

import math
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from clickstream_generator.config import Config
from clickstream_generator.dictionary import EventDictionary
from clickstream_generator.intensity import calculate_events_count, hour_factor


PAGE_START_DISTRIBUTION = [
    ("/home", 0.59),
    ("/product_a", 0.20),
    ("/product_b", 0.14),
    ("/cart", 0.04),
    ("/payment", 0.02),
    ("/confirmation", 0.01),
]

PAGE_TRANSITIONS = {
    "/home": [
        ("/home", 0.41),
        ("/product_a", 0.28),
        ("/product_b", 0.18),
        ("/cart", 0.04),
        (None, 0.09),
    ],
    "/product_a": [
        ("/home", 0.34),
        ("/product_a", 0.13),
        ("/product_b", 0.18),
        ("/cart", 0.27),
        (None, 0.08),
    ],
    "/product_b": [
        ("/home", 0.36),
        ("/product_a", 0.15),
        ("/product_b", 0.15),
        ("/cart", 0.27),
        (None, 0.07),
    ],
    "/cart": [
        ("/home", 0.20),
        ("/product_a", 0.12),
        ("/product_b", 0.10),
        ("/cart", 0.11),
        ("/payment", 0.42),
        (None, 0.05),
    ],
    "/payment": [
        ("/home", 0.12),
        ("/cart", 0.24),
        ("/payment", 0.14),
        ("/confirmation", 0.38),
        (None, 0.12),
    ],
    "/confirmation": [
        ("/home", 0.37),
        ("/product_a", 0.15),
        ("/product_b", 0.10),
        ("/confirmation", 0.05),
        (None, 0.33),
    ],
}


# Калибровочный ориентир из профиля сида: средний визит около 10 событий.
# Тиковый слой использует это число, чтобы переводить событийный бюджет в
# рождения визитов; тесты ниже защищают фактическую длину визита от дрейфа.
EXPECTED_VISIT_EVENTS = 10.0


class EventGenerator:
    """Генератор одного связанного визита."""

    def __init__(self, dictionary: EventDictionary, config: Config):
        self.dictionary = dictionary
        self.config = config
        self.rng = random.Random(config.seed)

    def _new_uuid(self) -> str:
        """Генерирует новый UUID."""
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _current_timestamp(self) -> str:
        """Возвращает текущую метку времени в формате JSONL."""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    def _format_timestamp(self, timestamp: datetime) -> str:
        """Форматирует запланированную метку времени для JSONL."""
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _weighted_choice(self, choices: list[tuple[Any, float]]) -> Any:
        """Разыгрывает значение по списку весов."""
        point = self.rng.random()
        cumulative = 0.0
        for value, weight in choices:
            cumulative += weight
            if point < cumulative:
                return value
        return choices[-1][0]

    def _generate_visit_path(self, max_events: int, min_events: int = 1) -> list[str]:
        """Генерирует путь визита по страницам с защитой от бесконечных петель."""
        page = self._weighted_choice(PAGE_START_DISTRIBUTION)
        path = []

        while page is not None and len(path) < max_events:
            path.append(page)
            page = self._weighted_choice(PAGE_TRANSITIONS[page])

        while len(path) < min_events and len(path) < max_events:
            transitions = [
                (next_page, weight)
                for next_page, weight in PAGE_TRANSITIONS[path[-1]]
                if next_page is not None
            ]
            path.append(self._weighted_choice(transitions))

        return path

    def _visit_pause_seconds(self) -> float:
        """Разыгрывает паузу между событиями визита."""
        pause = self.rng.lognormvariate(math.log(20.0), 0.9)
        return max(1.0, min(pause, 29 * 60.0))

    def _hour_factor(self, now: datetime | None = None) -> float:
        """Совместимый wrapper над расчётом часового коэффициента."""
        return hour_factor(now, self.config.model_timezone)

    def _calculate_events_count(self, now: datetime | None = None) -> int:
        """Совместимый wrapper над расчётом событийного бюджета."""
        return calculate_events_count(self.config, self.rng, now=now)

    def generate_batch(
        self,
        batch_size: int,
        planned_start_at: datetime | None = None,
        user_profile: dict[str, dict] | None = None,
    ) -> dict[str, list[dict]]:
        """Генерирует один визит с сохранением связей."""
        if not self.dictionary.browser_events:
            return {
                "browser_events": [],
                "location_events": [],
                "device_events": [],
                "geo_events": [],
            }

        batch = {
            "browser_events": [],
            "location_events": [],
            "device_events": [],
            "geo_events": [],
        }

        if batch_size <= 0:
            return batch

        max_visit_events = min(batch_size, self.config.max_session_events)
        min_visit_events = min(2, max_visit_events)
        visit_path = self._generate_visit_path(max_visit_events, min_visit_events)

        visit_candidates = [
            click_id for click_id, browser_events in self.dictionary.browser_by_click_id.items()
            if (
                len(browser_events) >= len(visit_path)
                and click_id in self.dictionary.device_by_click_id
                and click_id in self.dictionary.geo_by_click_id
                and all(
                    event["event_id"] in self.dictionary.location_by_event_id
                    for event in browser_events[:len(visit_path)]
                )
            )
        ]
        if visit_candidates:
            base_click_id = self.rng.choice(visit_candidates)
            base_browser_events = self.dictionary.browser_by_click_id[base_click_id][:len(visit_path)]
        else:
            base_browser = self.rng.choice(self.dictionary.browser_events)
            base_click_id = base_browser["click_id"]
            base_browser_events = [base_browser for _ in range(len(visit_path))]

        base_device = (
            user_profile["device"]
            if user_profile is not None
            else self.dictionary.device_by_click_id.get(base_click_id)
        )
        base_geo = (
            user_profile["geo"]
            if user_profile is not None
            else self.dictionary.geo_by_click_id.get(base_click_id)
        )
        new_click_id = self._new_uuid()
        planned_timestamp = planned_start_at or datetime.now(timezone.utc)
        if planned_timestamp.tzinfo is not None:
            planned_timestamp = planned_timestamp.astimezone(timezone.utc).replace(tzinfo=None)

        for base_browser, page_url_path in zip(base_browser_events, visit_path):
            base_location = self.dictionary.location_by_event_id.get(base_browser["event_id"])
            new_event_id = self._new_uuid()
            new_timestamp = self._format_timestamp(planned_timestamp)

            browser_event = {
                **base_browser,
                "event_id": new_event_id,
                "click_id": new_click_id,
                "event_timestamp": new_timestamp,
            }
            batch["browser_events"].append(browser_event)

            if base_location:
                location_event = {
                    **base_location,
                    "event_id": new_event_id,
                    "page_url": f"http://www.dummywebsite.com{page_url_path}",
                    "page_url_path": page_url_path,
                }
                batch["location_events"].append(location_event)

            if base_device:
                batch["device_events"].append({**base_device, "click_id": new_click_id})

            if base_geo:
                batch["geo_events"].append({**base_geo, "click_id": new_click_id})

            planned_timestamp += timedelta(seconds=self._visit_pause_seconds())

        return batch
