"""Тиковый слой генератора с активными визитами между вызовами."""

from dataclasses import dataclass
from datetime import datetime, timezone
from weakref import WeakKeyDictionary

from clickstream_generator.generation import EventGenerator


TOPICS = ("browser_events", "location_events", "device_events", "geo_events")


@dataclass
class ActiveVisit:
    """Запланированный визит, который выпускается по тикам."""

    batch: dict[str, list[dict]]
    timestamps: list[datetime]
    next_index: int = 0

    @property
    def is_finished(self) -> bool:
        return self.next_index >= len(self.timestamps)


def _empty_batch() -> dict[str, list[dict]]:
    return {topic: [] for topic in TOPICS}


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace(" ", "T"))


def _normalize_tick_time(tick_started_at: datetime | None) -> datetime:
    tick_time = tick_started_at or datetime.now(timezone.utc)
    if tick_time.tzinfo is not None:
        return tick_time.astimezone(timezone.utc).replace(tzinfo=None)
    return tick_time


class TickStreamGenerator:
    """Раскладывает визиты по тикам и хранит активные визиты."""

    def __init__(self, generator: EventGenerator):
        self.generator = generator
        self.active_visits: list[ActiveVisit] = []
        self._pending_event_budget = 0

    def generate_tick(
        self,
        event_budget: int,
        tick_started_at: datetime | None = None,
    ) -> dict[str, list[dict]]:
        """Возвращает события, созревшие к текущему тику."""
        tick_time = _normalize_tick_time(tick_started_at)
        tick_batch = _empty_batch()

        self._release_due_events(tick_time, tick_batch)
        self._drop_finished_visits()
        self._birth_visits(event_budget, tick_time)
        self._release_due_events(tick_time, tick_batch)
        self._drop_finished_visits()

        return tick_batch

    def _birth_visits(self, event_budget: int, tick_time: datetime) -> None:
        if len(self.active_visits) >= self.generator.config.max_active_sessions:
            self._pending_event_budget = 0
            return

        self._pending_event_budget += max(0, event_budget)

        while (
            self._pending_event_budget > 0
            and len(self.active_visits) < self.generator.config.max_active_sessions
        ):
            visit_batch = self.generator.generate_batch(
                self.generator.config.max_session_events,
                planned_start_at=tick_time,
            )
            timestamps = [
                _parse_timestamp(event["event_timestamp"])
                for event in visit_batch["browser_events"]
            ]
            if not timestamps:
                break

            self.active_visits.append(ActiveVisit(batch=visit_batch, timestamps=timestamps))
            self._pending_event_budget -= len(timestamps)

        if len(self.active_visits) >= self.generator.config.max_active_sessions:
            self._pending_event_budget = 0

    def _release_due_events(
        self,
        tick_time: datetime,
        tick_batch: dict[str, list[dict]],
    ) -> None:
        for visit in self.active_visits:
            while not visit.is_finished and visit.timestamps[visit.next_index] <= tick_time:
                event_index = visit.next_index
                for topic in TOPICS:
                    tick_batch[topic].append(visit.batch[topic][event_index])
                visit.next_index += 1

    def _drop_finished_visits(self) -> None:
        self.active_visits = [
            visit
            for visit in self.active_visits
            if not visit.is_finished
        ]


_STREAMS: WeakKeyDictionary[EventGenerator, TickStreamGenerator] = WeakKeyDictionary()


def generate_tick_batch(
    generator: EventGenerator,
    event_budget: int,
    tick_started_at: datetime | None = None,
) -> dict[str, list[dict]]:
    """Совместимый фасад тикового генератора.

    `event_budget` здесь трактуется как бюджет событий за тик. Если потолок
    активных визитов достигнут, новые рождения пропускаются и бюджет не
    переносится на следующие тики.
    """
    stream = _STREAMS.get(generator)
    if stream is None:
        stream = TickStreamGenerator(generator)
        _STREAMS[generator] = stream

    return stream.generate_tick(event_budget, tick_started_at=tick_started_at)
