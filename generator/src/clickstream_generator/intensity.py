"""Расчёт событийного бюджета тика."""

import math
import random
from datetime import datetime
from zoneinfo import ZoneInfo

from clickstream_generator.config import Config


POISSON_KNUTH_MAX_LAMBDA = 100.0


def hour_factor(now: datetime | None = None, model_timezone: str = "UTC") -> float:
    """Возвращает коэффициент интенсивности в зависимости от часа дня."""
    current = now
    if current is None:
        raise ValueError("now is required for hour_factor")
    if current.tzinfo is None:
        current = current.replace(tzinfo=ZoneInfo("UTC"))
    current = current.astimezone(ZoneInfo(model_timezone))
    hour = current.hour
    if 9 <= hour <= 18:
        return 1.2
    if 0 <= hour <= 5:
        return 0.7
    return 1.0


def _sample_poisson(lambda_tick: float, rng: random.Random) -> int:
    """Разыгрывает Poisson без underflow на больших λ."""
    if lambda_tick >= POISSON_KNUTH_MAX_LAMBDA:
        return max(0, round(rng.gauss(lambda_tick, math.sqrt(lambda_tick))))

    count = 0
    threshold = math.exp(-lambda_tick)
    product = 1.0
    while product > threshold:
        product *= rng.random()
        count += 1
    return count - 1


def calculate_events_count(
    config: Config,
    rng: random.Random,
    now: datetime | None = None,
) -> int:
    """Вычисляет количество событий для текущего тика (Poisson + jitter)."""
    model_time = now or config.model_t0
    factor = hour_factor(model_time, config.model_timezone)
    lambda_minute = config.lambda_base_per_min * factor
    model_tick_seconds = config.tick_seconds * config.model_time_speed
    lambda_tick = lambda_minute * (model_tick_seconds / 60.0)

    count = _sample_poisson(lambda_tick, rng)

    if config.jitter_pct > 0:
        jitter_factor = 1.0 + rng.uniform(
            -config.jitter_pct / 100.0,
            config.jitter_pct / 100.0,
        )
        count = int(count * jitter_factor)

    return max(config.min_events_per_tick, min(count, config.max_events_per_tick))
