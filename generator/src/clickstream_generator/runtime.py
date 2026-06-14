"""Тиковый слой генератора с активными визитами между вызовами."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from weakref import WeakKeyDictionary

from clickstream_generator.generation import EXPECTED_VISIT_EVENTS, EventGenerator
from clickstream_generator.state import GeneratorState


TOPICS = ("browser_events", "location_events", "device_events", "geo_events")
RESTART_VISIT_GRACE = timedelta(minutes=30)


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


def _datetime_to_state(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _timestamp_to_state_offset(started_at: datetime, timestamp: datetime) -> int:
    return int((timestamp - started_at).total_seconds() * 1_000_000)


def _format_event_timestamp(timestamp: datetime) -> str:
    return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")


def _stable_event_id(click_id: str, event_index: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{click_id}:{event_index}"))


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

    @property
    def active_visit_count(self) -> int:
        return len(self.active_visits)

    def to_state(
        self,
        tick: int,
        rng_state: tuple,
        last_batch_id: str,
        last_timestamp: datetime,
        model_timestamp: datetime | None = None,
        wall_timestamp: datetime | None = None,
        model_time_speed: float = 1.0,
        model_timezone: str = "UTC",
        model_t0: datetime | None = None,
        gen_seed: int | None = None,
    ) -> GeneratorState:
        """Возвращает JSON-сериализуемый снимок тикового слоя."""
        return GeneratorState(
            tick=tick,
            rng_state=rng_state,
            last_batch_id=last_batch_id,
            last_timestamp=last_timestamp,
            model_timestamp=model_timestamp,
            wall_timestamp=wall_timestamp,
            model_time_speed=model_time_speed,
            model_timezone=model_timezone,
            model_t0=model_t0,
            gen_seed=gen_seed,
            population=[
                {
                    "user_domain_id": user.user_domain_id,
                    "seed_click_id": user.seed_click_id,
                    "active_click_id": user.active_click_id,
                    "last_finished_at": _datetime_to_state(user.last_finished_at),
                }
                for user in self.population.users
            ],
            active_visits=[
                self._visit_to_state(visit)
                for visit in self.active_visits
            ],
            pending_visit_births=self._pending_visit_births,
        )

    def _visit_to_state(self, visit: ActiveVisit) -> dict:
        started_at = visit.timestamps[0]
        browser_events = visit.batch["browser_events"]
        location_events = visit.batch["location_events"]
        return {
            "user_domain_id": visit.user.user_domain_id if visit.user else None,
            "click_id": browser_events[0]["click_id"],
            "next_index": visit.next_index,
            "started_at": started_at.isoformat(),
            "offsets_us": [
                _timestamp_to_state_offset(started_at, timestamp)
                for timestamp in visit.timestamps
            ],
            "page_url_paths": [
                event["page_url_path"]
                for event in location_events
            ],
        }

    def restore_state(
        self,
        state: GeneratorState,
        resume_model_at: datetime | None = None,
        restarted_at: datetime | None = None,
    ) -> None:
        """Восстанавливает популяцию и активные визиты из state v2."""
        users = [
            self._user_from_state(item)
            for item in state.population
        ]
        users_by_id = {user.user_domain_id: user for user in users}

        self.population.users = users
        self.active_visits = []
        model_resume_time = (
            _normalize_tick_time(resume_model_at or restarted_at)
            if resume_model_at is not None or restarted_at is not None
            else None
        )
        for item in state.active_visits:
            visit = self._visit_from_state(item, users_by_id)
            if self._is_overdue_after_restart(visit, model_resume_time):
                self.population.finish_visit(
                    visit.user,
                    self._last_released_at(visit, state.model_timestamp),
                )
                continue
            self.active_visits.append(visit)
        self._pending_visit_births = state.pending_visit_births

    def _user_from_state(self, item: dict) -> UserProfile:
        seed_click_id = item["seed_click_id"]
        if seed_click_id not in self.generator.dictionary.device_by_click_id:
            raise ValueError(f"Unknown user seed_click_id: {seed_click_id}")
        if seed_click_id not in self.generator.dictionary.geo_by_click_id:
            raise ValueError(f"Unknown user geo seed_click_id: {seed_click_id}")

        user_domain_id = item["user_domain_id"]
        device = {
            **self.generator.dictionary.device_by_click_id[seed_click_id],
            "user_domain_id": user_domain_id,
        }
        geo = self.generator.dictionary.geo_by_click_id[seed_click_id]
        return UserProfile(
            user_domain_id=user_domain_id,
            seed_click_id=seed_click_id,
            device=device,
            geo=geo,
            active_click_id=item.get("active_click_id"),
            last_finished_at=(
                _parse_timestamp(item["last_finished_at"])
                if item.get("last_finished_at")
                else None
            ),
        )

    def _visit_from_state(
        self,
        item: dict,
        users_by_id: dict[str, UserProfile],
    ) -> ActiveVisit:
        user = users_by_id.get(item["user_domain_id"])
        if user is None:
            raise ValueError(f"Unknown active visit user: {item['user_domain_id']}")

        started_at = _parse_timestamp(item["started_at"])
        timestamps = [
            started_at + timedelta(microseconds=offset_us)
            for offset_us in item["offsets_us"]
        ]
        batch = self._compact_visit_batch(
            click_id=item["click_id"],
            user=user,
            timestamps=timestamps,
            page_url_paths=item["page_url_paths"],
        )
        return ActiveVisit(
            batch=batch,
            timestamps=timestamps,
            user=user,
            next_index=item["next_index"],
        )

    def _compact_visit_batch(
        self,
        click_id: str,
        user: UserProfile,
        timestamps: list[datetime],
        page_url_paths: list[str],
    ) -> dict[str, list[dict]]:
        batch = _empty_batch()
        browser_templates = self.generator.dictionary.browser_by_click_id.get(
            user.seed_click_id,
            self.generator.dictionary.browser_events,
        )

        for event_index, (timestamp, page_url_path) in enumerate(
            zip(timestamps, page_url_paths)
        ):
            browser_template = browser_templates[event_index % len(browser_templates)]
            location_template = self.generator.dictionary.location_by_event_id.get(
                browser_template["event_id"],
                self.generator.dictionary.location_events[0],
            )
            event_id = _stable_event_id(click_id, event_index)
            batch["browser_events"].append(
                {
                    **browser_template,
                    "event_id": event_id,
                    "click_id": click_id,
                    "event_timestamp": _format_event_timestamp(timestamp),
                }
            )
            batch["location_events"].append(
                {
                    **location_template,
                    "event_id": event_id,
                    "page_url": f"http://www.dummywebsite.com{page_url_path}",
                    "page_url_path": page_url_path,
                }
            )
            batch["device_events"].append({**user.device, "click_id": click_id})
            batch["geo_events"].append({**user.geo, "click_id": click_id})

        return batch

    def _last_released_at(
        self,
        visit: ActiveVisit,
        fallback: datetime,
    ) -> datetime:
        if visit.next_index > 0:
            return visit.timestamps[visit.next_index - 1]
        if fallback.tzinfo is not None:
            return fallback.astimezone(timezone.utc).replace(tzinfo=None)
        return fallback

    def _is_overdue_after_restart(
        self,
        visit: ActiveVisit,
        restarted_time: datetime | None,
    ) -> bool:
        if restarted_time is None or visit.is_finished:
            return False
        next_timestamp = visit.timestamps[visit.next_index]
        return restarted_time - next_timestamp > RESTART_VISIT_GRACE

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
