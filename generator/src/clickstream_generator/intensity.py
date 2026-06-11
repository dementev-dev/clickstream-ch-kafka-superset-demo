"""Расчёт событийного бюджета тика."""

import math
import random
from datetime import datetime, timezone

from clickstream_generator.config import Config


def hour_factor(now: datetime | None = None) -> float:
    """Возвращает коэффициент интенсивности в зависимости от часа дня."""
    current = now or datetime.now(timezone.utc)
    hour = current.hour
    if 9 <= hour <= 18:
        return 1.2
    if 0 <= hour <= 5:
        return 0.7
    return 1.0


def calculate_events_count(config: Config, rng: random.Random) -> int:
    """Вычисляет количество событий для текущего тика (Poisson + jitter)."""
    lambda_minute = config.lambda_base_per_min * hour_factor()
    lambda_tick = lambda_minute * (config.tick_seconds / 60.0)

    count = 0
    threshold = math.exp(-lambda_tick)
    product = 1.0
    while product > threshold:
        product *= rng.random()
        count += 1
    count -= 1

    if config.jitter_pct > 0:
        jitter_factor = 1.0 + rng.uniform(
            -config.jitter_pct / 100.0,
            config.jitter_pct / 100.0,
        )
        count = int(count * jitter_factor)

    return max(config.min_events_per_tick, min(count, config.max_events_per_tick))
