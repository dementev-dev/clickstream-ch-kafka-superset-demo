"""Конфигурация генератора из переменных окружения."""

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _parse_model_timestamp(value: str) -> datetime:
    """Разбирает ISO-метку модельного времени и нормализует её к UTC."""
    normalized = value.replace("Z", "+00:00")
    timestamp = datetime.fromisoformat(normalized)
    if timestamp.tzinfo is None:
        raise ValueError("GEN_MODEL_T0 must include timezone")
    return timestamp.astimezone(timezone.utc)


@dataclass(frozen=True)
class Config:
    """Конфигурация генератора из переменных окружения."""

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    )
    tick_seconds: int = field(
        default_factory=lambda: int(os.getenv("GEN_TICK_SECONDS", "5"))
    )
    lambda_base_per_min: int = field(
        default_factory=lambda: int(os.getenv("GEN_LAMBDA_BASE_PER_MIN", "30"))
    )
    jitter_pct: int = field(
        default_factory=lambda: int(os.getenv("GEN_JITTER_PCT", "20"))
    )
    min_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MIN_EVENTS_PER_TICK", "1"))
    )
    max_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MAX_EVENTS_PER_TICK", "50"))
    )
    max_session_events: int = field(
        default_factory=lambda: int(os.getenv("GEN_MAX_SESSION_EVENTS", "30"))
    )
    max_active_sessions: int = field(
        default_factory=lambda: int(os.getenv("GEN_MAX_ACTIVE_SESSIONS", "200"))
    )
    population_max: int = field(
        default_factory=lambda: int(os.getenv("GEN_POPULATION_MAX", "300"))
    )
    p_new_user: float = field(
        default_factory=lambda: float(os.getenv("GEN_P_NEW_USER", "0.15"))
    )
    min_return_minutes: int = field(
        default_factory=lambda: int(os.getenv("GEN_MIN_RETURN_MINUTES", "30"))
    )
    data_dir: Path = field(
        default_factory=lambda: Path(os.getenv("GEN_DATA_DIR", "/data"))
    )
    seed: int | None = field(
        default_factory=lambda: int(os.getenv("GEN_SEED"))
        if os.getenv("GEN_SEED")
        else None
    )
    enabled: bool = field(
        default_factory=lambda: os.getenv("GEN_ENABLED", "true").lower() == "true"
    )
    metrics_port: int = field(
        default_factory=lambda: int(os.getenv("GEN_METRICS_PORT", "9109"))
    )
    state_enabled: bool = field(
        default_factory=lambda: os.getenv("GEN_STATE_ENABLED", "true").lower() == "true"
    )
    state_reset: bool = field(
        default_factory=lambda: os.getenv("GEN_STATE_RESET", "false").lower() == "true"
    )
    model_t0: datetime = field(
        default_factory=lambda: _parse_model_timestamp(
            os.getenv("GEN_MODEL_T0", "2026-01-01T00:00:00+00:00")
        )
    )
    model_t_end: datetime | None = field(
        default_factory=lambda: _parse_optional_model_timestamp(
            os.getenv("GEN_MODEL_T_END")
        )
    )
    model_timezone: str = field(
        default_factory=lambda: os.getenv("GEN_MODEL_TIMEZONE", "UTC")
    )
    model_time_speed: float = field(
        default_factory=lambda: float(os.getenv("GEN_MODEL_TIME_SPEED", "1"))
    )
    run_mode: str = field(
        default_factory=lambda: os.getenv("GEN_RUN_MODE", "live")
    )

    def __post_init__(self):
        if self.tick_seconds < 1:
            raise ValueError("GEN_TICK_SECONDS must be >= 1")
        if self.lambda_base_per_min < 1:
            raise ValueError("GEN_LAMBDA_BASE_PER_MIN must be >= 1")
        if self.max_session_events < 1:
            raise ValueError("GEN_MAX_SESSION_EVENTS must be >= 1")
        if self.max_active_sessions < 1:
            raise ValueError("GEN_MAX_ACTIVE_SESSIONS must be >= 1")
        if self.population_max < 1:
            raise ValueError("GEN_POPULATION_MAX must be >= 1")
        if not 0 <= self.p_new_user <= 1:
            raise ValueError("GEN_P_NEW_USER must be between 0 and 1")
        if self.min_return_minutes < 0:
            raise ValueError("GEN_MIN_RETURN_MINUTES must be >= 0")
        if self.max_active_sessions >= self.population_max:
            raise ValueError("GEN_MAX_ACTIVE_SESSIONS must be < GEN_POPULATION_MAX")
        if not self.data_dir.exists():
            raise ValueError(f"Data directory does not exist: {self.data_dir}")
        if self.model_t0.tzinfo is None:
            raise ValueError("GEN_MODEL_T0 must include timezone")
        object.__setattr__(
            self,
            "model_t0",
            self.model_t0.astimezone(timezone.utc),
        )
        try:
            ZoneInfo(self.model_timezone)
        except ZoneInfoNotFoundError as e:
            raise ValueError(f"Unknown GEN_MODEL_TIMEZONE: {self.model_timezone}") from e
        if self.model_time_speed <= 0:
            raise ValueError("GEN_MODEL_TIME_SPEED must be > 0")
        if self.run_mode not in {"live", "backfill"}:
            raise ValueError("GEN_RUN_MODE must be live or backfill")
        if self.model_t_end is not None:
            object.__setattr__(
                self,
                "model_t_end",
                self.model_t_end.astimezone(timezone.utc),
            )
        if self.run_mode == "backfill":
            if self.model_t_end is None:
                raise ValueError("GEN_MODEL_T_END is required for backfill")
            if self.model_t_end <= self.model_t0:
                raise ValueError("GEN_MODEL_T_END must be after GEN_MODEL_T0")


def _parse_optional_model_timestamp(value: str | None) -> datetime | None:
    """Разбирает необязательную ISO-метку модельного времени."""
    if not value:
        return None
    return _parse_model_timestamp(value)
