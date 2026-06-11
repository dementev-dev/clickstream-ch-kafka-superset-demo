"""Конфигурация генератора из переменных окружения."""

import os
from dataclasses import dataclass, field
from pathlib import Path


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
        default_factory=lambda: int(os.getenv("GEN_LAMBDA_BASE_PER_MIN", "200"))
    )
    jitter_pct: int = field(
        default_factory=lambda: int(os.getenv("GEN_JITTER_PCT", "20"))
    )
    min_events_per_tick: int = field(
        default_factory=lambda: int(os.getenv("GEN_MIN_EVENTS_PER_TICK", "5"))
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
