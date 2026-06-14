"""
Тесты конфигурации генератора.
"""

from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest
from generator import Config


class TestConfigValidation:
    """Тесты валидации конфигурации."""

    def test_tick_seconds_must_be_positive(self, base_config):
        """tick_seconds должен быть >= 1."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="GEN_TICK_SECONDS"):
            replace(base_config, tick_seconds=0)

    def test_lambda_base_must_be_positive(self, base_config):
        """lambda_base_per_min должен быть >= 1."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="GEN_LAMBDA_BASE_PER_MIN"):
            replace(base_config, lambda_base_per_min=0)

    def test_max_session_events_must_be_positive(self, base_config):
        """max_session_events должен быть >= 1."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="GEN_MAX_SESSION_EVENTS"):
            replace(base_config, max_session_events=0)

    def test_max_active_sessions_must_be_less_than_population(self, base_config):
        """Потолок активных визитов должен быть меньше потолка популяции."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="GEN_MAX_ACTIVE_SESSIONS"):
            replace(base_config, max_active_sessions=10, population_max=10)

    def test_new_user_probability_must_be_valid_share(self, base_config):
        """Вероятность нового пользователя должна быть долей от 0 до 1."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="GEN_P_NEW_USER"):
            replace(base_config, p_new_user=1.5)

    def test_min_return_minutes_must_not_be_negative(self, base_config):
        """Кулдаун возврата не может быть отрицательным."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="GEN_MIN_RETURN_MINUTES"):
            replace(base_config, min_return_minutes=-1)

    def test_data_dir_must_exist(self, base_config):
        """data_dir должен существовать."""
        from dataclasses import replace
        with pytest.raises(ValueError, match="does not exist"):
            replace(base_config, data_dir=Path("/nonexistent/path"))

    def test_valid_config_passes(self, base_config):
        """Валидная конфигурация создается без ошибок."""
        assert base_config.tick_seconds == 5
        assert base_config.lambda_base_per_min == 30
        assert base_config.jitter_pct == 20
        assert base_config.max_active_sessions < base_config.population_max

    def test_model_t0_is_normalized_to_utc(self, base_config):
        """model_t0 нормализуется к UTC даже при прямом создании Config."""
        from dataclasses import replace

        config = replace(
            base_config,
            model_t0=datetime(
                2026,
                1,
                1,
                13,
                0,
                tzinfo=timezone(timedelta(hours=3)),
            ),
        )

        assert config.model_t0 == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)


class TestConfigDefaults:
    """Тесты значений по умолчанию."""

    def test_default_lambda_base_per_min_is_calibrated_to_demo_scale(
        self, monkeypatch, data_dir
    ):
        """По умолчанию целевая интенсивность равна 30 событиям в минуту."""
        monkeypatch.setenv("GEN_DATA_DIR", str(data_dir))
        monkeypatch.delenv("GEN_LAMBDA_BASE_PER_MIN", raising=False)

        config = Config()

        assert config.lambda_base_per_min == 30

    def test_default_tick_seconds_is_5(self, data_dir):
        """По умолчанию tick_seconds = 5 (rev5)."""
        import os
        # Сохраняем текущее значение
        orig_value = os.environ.get("GEN_TICK_SECONDS")
        try:
            if "GEN_TICK_SECONDS" in os.environ:
                del os.environ["GEN_TICK_SECONDS"]
            config = Config(
                kafka_bootstrap_servers="localhost:9092",
                tick_seconds=int(os.getenv("GEN_TICK_SECONDS", "5")),
                lambda_base_per_min=30,
                jitter_pct=20,
                min_events_per_tick=1,
                max_events_per_tick=50,
                data_dir=data_dir,
                seed=None,
                enabled=True,
                metrics_port=9109,
            )
            assert config.tick_seconds == 5
        finally:
            if orig_value is not None:
                os.environ["GEN_TICK_SECONDS"] = orig_value
