"""Тиковый слой генератора с активными визитами между вызовами."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from weakref import WeakKeyDictionary

from clickstream_generator.generation import EXPECTED_VISIT_EVENTS, EventGenerator


TOPICS = ("browser_events", "location_events", "device_events", "geo_events")


@dataclass
class UserProfile:
    """Постоянный профиль пользователя между визитами."""

    user_domain_id: str
    seed_click_id: str
    device: dict
    geo: dict
    active_click_id: str | None = None
    last_finished_at: datetime | None = None

    @property
    def is_active(self) -> bool:
        return self.active_click_id is not None


@dataclass
class ActiveVisit:
    """Запланированный визит, который выпускается по тикам."""

    batch: dict[str, list[dict]]
    timestamps: list[datetime]
    user: UserProfile | None = None
    next_index: int = 0

    @property
    def is_finished(self) -> bool:
        return self.next_index >= len(self.timestamps)


class UserPopulation:
    """Ограниченная популяция пользователей для тикового потока."""

    def __init__(self, generator: EventGenerator):
        self.generator = generator
        self.users: list[UserProfile] = [
            self._create_user()
            for _ in range(generator.config.population_max)
        ]

    def choose_for_visit(self, tick_time: datetime) -> UserProfile | None:
        """Выбирает пользователя без активного визита и кулдауна."""
        available = [
            user for user in self.users
            if self._is_available(user, tick_time)
        ]
        if not available:
            return self._rotate_new_user()
        if self.generator.rng.random() < self.generator.config.p_new_user:
            return self._rotate_new_user() or self.generator.rng.choice(available)
        return self.generator.rng.choice(available)

    def start_visit(self, user: UserProfile, click_id: str) -> None:
        user.active_click_id = click_id

    def finish_visit(self, user: UserProfile | None, finished_at: datetime) -> None:
        if user is None:
            return
        user.active_click_id = None
        user.last_finished_at = finished_at

    def _create_user(self) -> UserProfile:
        seed_click_id = self.generator.rng.choice(self._profile_seed_click_ids())
        device = {
            **self.generator.dictionary.device_by_click_id[seed_click_id],
            "user_domain_id": self.generator._new_uuid(),
        }
        geo = self.generator.dictionary.geo_by_click_id[seed_click_id]
        return UserProfile(
            user_domain_id=device["user_domain_id"],
            seed_click_id=seed_click_id,
            device=device,
            geo=geo,
        )

    def _rotate_new_user(self) -> UserProfile | None:
        inactive_users = [user for user in self.users if not user.is_active]
        if not inactive_users:
            return None

        new_user = self._create_user()
        victim = min(
            inactive_users,
            key=lambda user: user.last_finished_at or datetime.min,
        )
        self.users[self.users.index(victim)] = new_user
        return new_user

    def _is_available(self, user: UserProfile, tick_time: datetime) -> bool:
        if user.is_active:
            return False
        if user.last_finished_at is None:
            return True
        cooldown = timedelta(minutes=self.generator.config.min_return_minutes)
        return tick_time - user.last_finished_at >= cooldown

    def _profile_seed_click_ids(self) -> list[str]:
        return [
            click_id
            for click_id in self.generator.dictionary.device_by_click_id
            if click_id in self.generator.dictionary.geo_by_click_id
        ]


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
        self.population = UserPopulation(generator)
        self._pending_visit_births = 0.0

    @property
    def population_size(self) -> int:
        return len(self.population.users)

    @property
    def population_user_ids(self) -> set[str]:
        return {user.user_domain_id for user in self.population.users}

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
            self._pending_visit_births = 0.0
            return

        self._pending_visit_births += max(0, event_budget) / EXPECTED_VISIT_EVENTS

        while (
            self._pending_visit_births >= 1.0
            and len(self.active_visits) < self.generator.config.max_active_sessions
        ):
            user = self.population.choose_for_visit(tick_time)
            if user is None:
                self._pending_visit_births = 0.0
                return

            visit_batch = self.generator.generate_batch(
                self.generator.config.max_session_events,
                planned_start_at=tick_time,
                user_profile={"device": user.device, "geo": user.geo},
            )
            timestamps = [
                _parse_timestamp(event["event_timestamp"])
                for event in visit_batch["browser_events"]
            ]
            if not timestamps:
                break

            click_id = visit_batch["browser_events"][0]["click_id"]
            self.population.start_visit(user, click_id)
            self.active_visits.append(
                ActiveVisit(batch=visit_batch, timestamps=timestamps, user=user)
            )
            self._pending_visit_births -= 1.0

        if len(self.active_visits) >= self.generator.config.max_active_sessions:
            self._pending_visit_births = 0.0

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
        for visit in self.active_visits:
            if visit.is_finished:
                self.population.finish_visit(visit.user, visit.timestamps[-1])

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
