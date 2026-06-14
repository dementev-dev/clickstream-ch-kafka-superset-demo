"""Сериализуемое состояние генератора."""

import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


logger = logging.getLogger("generator")


STATE_VERSION = "2.0"


def _nested_list_to_tuple(obj):
    """Рекурсивно преобразует list в tuple для восстановления RNG state."""
    if isinstance(obj, list):
        return tuple(_nested_list_to_tuple(x) for x in obj)
    return obj


def _require_keys(item: dict, keys: tuple[str, ...], label: str) -> None:
    missing = [key for key in keys if key not in item]
    if missing:
        raise ValueError(f"{label} missing fields: {', '.join(missing)}")


def _parse_aware_utc(value: str, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError(f"{label} must include timezone")
    return timestamp.astimezone(timezone.utc)


def _validate_resume_fields(data: dict) -> None:
    _require_keys(
        data,
        (
            "model_timestamp",
            "wall_timestamp",
            "model_time_speed",
            "model_timezone",
            "model_t0",
            "gen_seed",
        ),
        "state",
    )
    _parse_aware_utc(data["model_timestamp"], "model_timestamp")
    _parse_aware_utc(data["wall_timestamp"], "wall_timestamp")
    _parse_aware_utc(data["model_t0"], "model_t0")

    model_time_speed = data["model_time_speed"]
    if (
        isinstance(model_time_speed, bool)
        or not isinstance(model_time_speed, int | float)
        or model_time_speed <= 0
    ):
        raise ValueError("model_time_speed must be a positive number")

    model_timezone = data["model_timezone"]
    if not isinstance(model_timezone, str) or not model_timezone:
        raise ValueError("model_timezone must be a string")
    try:
        ZoneInfo(model_timezone)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"unknown model_timezone: {model_timezone}") from e

    gen_seed = data["gen_seed"]
    if gen_seed is not None:
        if isinstance(gen_seed, bool) or not isinstance(gen_seed, int):
            raise ValueError("gen_seed must be an integer or null")


def _validate_v2_payload(data: dict) -> None:
    _validate_resume_fields(data)
    population = data.get("population")
    active_visits = data.get("active_visits")
    pending_visit_births = data.get("pending_visit_births", 0.0)

    if not isinstance(population, list):
        raise ValueError("population must be a list")
    if not population:
        raise ValueError("population must be non-empty")
    if not isinstance(active_visits, list):
        raise ValueError("active_visits must be a list")
    if not isinstance(pending_visit_births, int | float):
        raise ValueError("pending_visit_births must be a number")
    if not 0 <= pending_visit_births < 1_000_000:
        raise ValueError("pending_visit_births is out of range")

    users_by_id = {}
    for index, user in enumerate(population):
        if not isinstance(user, dict):
            raise ValueError(f"population[{index}] must be an object")
        _require_keys(user, ("user_domain_id", "seed_click_id"), f"population[{index}]")
        user_domain_id = user["user_domain_id"]
        seed_click_id = user["seed_click_id"]
        active_click_id = user.get("active_click_id")
        last_finished_at = user.get("last_finished_at")
        if not isinstance(user_domain_id, str) or not user_domain_id:
            raise ValueError(f"population[{index}].user_domain_id must be a string")
        if not isinstance(seed_click_id, str) or not seed_click_id:
            raise ValueError(f"population[{index}].seed_click_id must be a string")
        if active_click_id is not None and not isinstance(active_click_id, str):
            raise ValueError(f"population[{index}].active_click_id must be a string or null")
        if last_finished_at is not None:
            if not isinstance(last_finished_at, str):
                raise ValueError(f"population[{index}].last_finished_at must be a string or null")
            datetime.fromisoformat(last_finished_at)
        if user_domain_id in users_by_id:
            raise ValueError(f"duplicate population user_domain_id: {user_domain_id}")
        users_by_id[user_domain_id] = user

    active_visit_pairs = set()
    for index, visit in enumerate(active_visits):
        if not isinstance(visit, dict):
            raise ValueError(f"active_visits[{index}] must be an object")
        _require_keys(
            visit,
            (
                "user_domain_id",
                "click_id",
                "next_index",
                "started_at",
                "offsets_us",
                "page_url_paths",
            ),
            f"active_visits[{index}]",
        )
        user_domain_id = visit["user_domain_id"]
        click_id = visit["click_id"]
        offsets = visit["offsets_us"]
        page_url_paths = visit["page_url_paths"]
        next_index = visit["next_index"]
        if not isinstance(user_domain_id, str) or user_domain_id not in users_by_id:
            raise ValueError(f"active_visits[{index}].user_domain_id is unknown")
        if not isinstance(click_id, str) or not click_id:
            raise ValueError(f"active_visits[{index}].click_id must be a string")
        if not isinstance(offsets, list) or not offsets:
            raise ValueError(f"active_visits[{index}].offsets_us must be a non-empty list")
        if not all(isinstance(offset, int) and offset >= 0 for offset in offsets):
            raise ValueError(f"active_visits[{index}].offsets_us must contain non-negative integers")
        if offsets != sorted(offsets):
            raise ValueError(f"active_visits[{index}].offsets_us must be sorted")
        if not isinstance(page_url_paths, list) or len(page_url_paths) != len(offsets):
            raise ValueError(
                f"active_visits[{index}].page_url_paths must match offsets_us length"
            )
        if not all(isinstance(path, str) and path.startswith("/") for path in page_url_paths):
            raise ValueError(f"active_visits[{index}].page_url_paths must contain paths")
        if not isinstance(next_index, int) or not 0 <= next_index <= len(offsets):
            raise ValueError(f"active_visits[{index}].next_index is out of range")
        datetime.fromisoformat(visit["started_at"])

        user_active_click_id = users_by_id[user_domain_id].get("active_click_id")
        if user_active_click_id != click_id:
            raise ValueError(
                f"population active_click_id conflicts with active_visits[{index}]"
            )
        active_visit_pairs.add((user_domain_id, click_id))

    for user_domain_id, user in users_by_id.items():
        active_click_id = user.get("active_click_id")
        if active_click_id and (user_domain_id, active_click_id) not in active_visit_pairs:
            raise ValueError(
                f"population user {user_domain_id} has active_click_id without active visit"
            )


@dataclass
class GeneratorState:
    """Состояние генератора для восстановления после рестарта."""

    tick: int
    rng_state: tuple
    last_batch_id: str
    last_timestamp: datetime
    version: str = STATE_VERSION
    model_timestamp: datetime | None = None
    wall_timestamp: datetime | None = None
    model_time_speed: float = 1.0
    model_timezone: str = "UTC"
    model_t0: datetime | None = None
    gen_seed: int | None = None
    population: list[dict] = field(default_factory=list)
    active_visits: list[dict] = field(default_factory=list)
    pending_visit_births: float = 0.0

    def __post_init__(self) -> None:
        if self.model_timestamp is None:
            self.model_timestamp = self._as_aware_utc(self.last_timestamp)
        if self.wall_timestamp is None:
            self.wall_timestamp = self._as_aware_utc(self.last_timestamp)
        if self.model_t0 is None:
            self.model_t0 = self.model_timestamp

    @staticmethod
    def _as_aware_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def to_dict(self) -> dict:
        """Конвертирует в словарь для JSON-сериализации."""
        return {
            "tick": self.tick,
            "rng_state": self.rng_state,
            "last_batch_id": self.last_batch_id,
            "last_timestamp": self.last_timestamp.isoformat(),
            "model_timestamp": self.model_timestamp.isoformat(),
            "wall_timestamp": self.wall_timestamp.isoformat(),
            "model_time_speed": self.model_time_speed,
            "model_timezone": self.model_timezone,
            "model_t0": self.model_t0.isoformat(),
            "gen_seed": self.gen_seed,
            "version": self.version,
            "population": self.population,
            "active_visits": self.active_visits,
            "pending_visit_births": self.pending_visit_births,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GeneratorState":
        """Создаёт состояние из словаря."""
        try:
            if not isinstance(data, dict):
                raise ValueError("state must be an object")
            version = data.get("version", "1.0")
            if version != STATE_VERSION:
                logger.warning(
                    "Unsupported generator state version %s, will start fresh",
                    version,
                )
                raise ValueError(f"unsupported state version: {version}")
            _validate_v2_payload(data)
            model_timestamp = _parse_aware_utc(
                data["model_timestamp"],
                "model_timestamp",
            )
            wall_timestamp = _parse_aware_utc(data["wall_timestamp"], "wall_timestamp")
            model_t0 = _parse_aware_utc(data["model_t0"], "model_t0")

            rng_state_raw = data.get("rng_state")
            if not rng_state_raw:
                logger.warning("State missing rng_state field")
                raise ValueError("rng_state is missing")

            rng_state = _nested_list_to_tuple(rng_state_raw)

            if not isinstance(rng_state, tuple):
                logger.warning(f"rng_state is not tuple: {type(rng_state)}")
                raise ValueError("rng_state must be tuple")

            if len(rng_state) < 2:
                logger.warning(f"rng_state has insufficient length: {len(rng_state)}")
                raise ValueError("rng_state has insufficient length")

            test_rng = random.Random()
            test_rng.setstate(rng_state)

            return cls(
                tick=data.get("tick", 0),
                rng_state=rng_state,
                last_batch_id=data.get("last_batch_id", ""),
                last_timestamp=datetime.fromisoformat(
                    data.get("last_timestamp", data["model_timestamp"])
                ),
                version=version,
                model_timestamp=model_timestamp,
                wall_timestamp=wall_timestamp,
                model_time_speed=float(data["model_time_speed"]),
                model_timezone=data["model_timezone"],
                model_t0=model_t0,
                gen_seed=data["gen_seed"],
                population=data.get("population", []),
                active_visits=data.get("active_visits", []),
                pending_visit_births=data.get("pending_visit_births", 0.0),
            )
        except Exception as e:
            logger.warning(f"Invalid state format, will start fresh: {e}")
            raise ValueError(f"Invalid state: {e}")

    @classmethod
    def from_dict_safe(cls, data: dict) -> "GeneratorState | None":
        """Безопасная загрузка state с graceful degradation."""
        try:
            return cls.from_dict(data)
        except Exception:
            return None
