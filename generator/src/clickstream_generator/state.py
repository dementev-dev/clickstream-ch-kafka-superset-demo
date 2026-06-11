"""Сериализуемое состояние генератора."""

import logging
import random
from dataclasses import dataclass
from datetime import datetime


logger = logging.getLogger("generator")


def _nested_list_to_tuple(obj):
    """Рекурсивно преобразует list в tuple для восстановления RNG state."""
    if isinstance(obj, list):
        return tuple(_nested_list_to_tuple(x) for x in obj)
    return obj


@dataclass
class GeneratorState:
    """Состояние генератора для восстановления после рестарта."""

    tick: int
    rng_state: tuple
    last_batch_id: str
    last_timestamp: datetime
    version: str = "1.0"

    def to_dict(self) -> dict:
        """Конвертирует в словарь для JSON-сериализации."""
        return {
            "tick": self.tick,
            "rng_state": self.rng_state,
            "last_batch_id": self.last_batch_id,
            "last_timestamp": self.last_timestamp.isoformat(),
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "GeneratorState":
        """Создаёт состояние из словаря."""
        try:
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
                    data.get("last_timestamp", "1970-01-01T00:00:00+00:00")
                ),
                version=data.get("version", "1.0"),
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
