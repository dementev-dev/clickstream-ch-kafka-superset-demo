"""
Тесты генерации событий.
"""

import uuid
from dataclasses import replace
from datetime import datetime, timedelta

import pytest
from generator import EventGenerator, EventDictionary, TickStreamGenerator, generate_tick_batch


ALLOWED_PAGE_PATHS = {
    "/home",
    "/product_a",
    "/product_b",
    "/cart",
    "/payment",
    "/confirmation",
}


def _parse_event_timestamps(batch):
    return [
        datetime.fromisoformat(event["event_timestamp"].replace(" ", "T"))
        for event in batch["browser_events"]
    ]


def _page_path_visits(generator, visits_count: int):
    return [
        [
            event["page_url_path"]
            for event in generator.generate_batch(30)["location_events"]
        ]
        for _ in range(visits_count)
    ]


class TestEventGeneration:
    """Тесты генерации событий."""

    def test_event_dictionary_loads(self, event_dictionary):
        """Словарь событий загружается корректно."""
        assert len(event_dictionary.browser_events) == 1000
        assert len(event_dictionary.location_events) == 1000
        assert len(event_dictionary.device_events) == 1000
        assert len(event_dictionary.geo_events) == 1000

    def test_dictionary_consistency(self, event_dictionary):
        """Все записи в словаре имеют корректные связи."""
        browser_event_ids = {e["event_id"] for e in event_dictionary.browser_events}
        browser_click_ids = {e["click_id"] for e in event_dictionary.browser_events}

        location_orphaned = sum(
            1 for loc in event_dictionary.location_events
            if loc["event_id"] not in browser_event_ids
        )
        device_orphaned = sum(
            1 for dev in event_dictionary.device_events
            if dev["click_id"] not in browser_click_ids
        )
        geo_orphaned = sum(
            1 for geo in event_dictionary.geo_events
            if geo["click_id"] not in browser_click_ids
        )

        assert location_orphaned == 0, f"Found {location_orphaned} orphaned location records"
        assert device_orphaned == 0, f"Found {device_orphaned} orphaned device records"
        assert geo_orphaned == 0, f"Found {geo_orphaned} orphaned geo records"

    def test_generate_batch_structure(self, event_dictionary, base_config):
        """Батч имеет правильную структуру."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(10)

        assert "browser_events" in batch
        assert "location_events" in batch
        assert "device_events" in batch
        assert "geo_events" in batch

    def test_generate_batch_respects_requested_visit_budget(self, event_dictionary, base_config):
        """Визит не превышает запрошенный бюджет событий."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(10)

        browser_count = len(batch["browser_events"])

        assert 1 <= browser_count <= 10
        assert len(batch["location_events"]) == browser_count
        assert len(batch["device_events"]) == browser_count
        assert len(batch["geo_events"]) == browser_count

    def test_generate_tick_batch_uses_requested_event_budget(self, event_dictionary, base_config):
        """Тиковый батч трактует входное число как событийный бюджет."""
        generator = EventGenerator(event_dictionary, base_config)

        batch = generate_tick_batch(generator, 20)

        browser_count = len(batch["browser_events"])

        assert 1 <= browser_count <= 20
        assert len(batch["location_events"]) == browser_count
        assert len(batch["device_events"]) == browser_count
        assert len(batch["geo_events"]) == browser_count
        assert len({event["click_id"] for event in batch["browser_events"]}) == browser_count

    def test_tick_stream_keeps_long_window_intensity_near_event_budget(
        self, event_dictionary, base_config
    ):
        """Длинное окно не разгоняется от событийного бюджета к бюджету визитов."""
        config = replace(
            base_config,
            tick_seconds=5,
            jitter_pct=0,
            min_events_per_tick=17,
            max_events_per_tick=17,
            max_active_sessions=10_000,
            population_max=10_001,
        )
        generator = EventGenerator(event_dictionary, config)
        stream = TickStreamGenerator(generator)
        tick_at = datetime(2026, 6, 11, 12, 0)
        ticks_count = 12 * 60
        total_events = 0

        for tick_index in range(ticks_count):
            batch = stream.generate_tick(
                event_budget=17,
                tick_started_at=tick_at + timedelta(seconds=5 * tick_index),
            )
            total_events += len(batch["browser_events"])

        events_per_minute = total_events / (ticks_count * config.tick_seconds / 60)

        assert events_per_minute < 300

    def test_tick_stream_keeps_active_visit_between_ticks(self, event_dictionary, base_config):
        """Один визит может выпускать события в нескольких последовательных тиках."""
        config = replace(base_config, tick_seconds=60, max_session_events=5)
        generator = EventGenerator(event_dictionary, config)
        stream = TickStreamGenerator(generator)
        first_tick_at = datetime(2026, 6, 11, 12, 0)

        first_tick = stream.generate_tick(event_budget=1, tick_started_at=first_tick_at)
        second_tick = stream.generate_tick(
            event_budget=0,
            tick_started_at=first_tick_at + timedelta(seconds=config.tick_seconds),
        )

        first_click_id = first_tick["browser_events"][0]["click_id"]
        second_click_ids = {
            event["click_id"]
            for event in second_tick["browser_events"]
        }
        second_timestamps = _parse_event_timestamps(second_tick)

        assert first_click_id in second_click_ids
        assert all(
            timestamp < first_tick_at + timedelta(seconds=config.tick_seconds)
            for timestamp in second_timestamps
        )

    def test_tick_stream_releases_only_matured_events(self, event_dictionary, base_config):
        """Тик не выпускает будущие события активного визита."""
        generator = EventGenerator(event_dictionary, base_config)
        stream = TickStreamGenerator(generator)
        tick_at = datetime(2026, 6, 11, 12, 0)

        first_tick = stream.generate_tick(event_budget=1, tick_started_at=tick_at)
        same_time_tick = stream.generate_tick(event_budget=0, tick_started_at=tick_at)

        assert len(first_tick["browser_events"]) == 1
        assert same_time_tick["browser_events"] == []
        assert same_time_tick["location_events"] == []
        assert same_time_tick["device_events"] == []
        assert same_time_tick["geo_events"] == []

    def test_tick_stream_does_not_reemit_finished_visit(self, event_dictionary, base_config):
        """Завершённый визит больше не выпускает события в следующих тиках."""
        config = replace(base_config, max_session_events=2)
        generator = EventGenerator(event_dictionary, config)
        stream = TickStreamGenerator(generator)
        tick_at = datetime(2026, 6, 11, 12, 0)

        first_tick = stream.generate_tick(event_budget=1, tick_started_at=tick_at)
        final_tick = stream.generate_tick(
            event_budget=0,
            tick_started_at=tick_at + timedelta(hours=1),
        )
        later_tick = stream.generate_tick(
            event_budget=0,
            tick_started_at=tick_at + timedelta(hours=2),
        )

        click_id = first_tick["browser_events"][0]["click_id"]

        assert {event["click_id"] for event in final_tick["browser_events"]} == {click_id}
        assert later_tick["browser_events"] == []

    def test_tick_stream_drops_births_when_active_limit_is_reached(
        self, event_dictionary, base_config
    ):
        """При заполненном потолке новые рождения пропускаются без накопления бюджета."""
        config = replace(
            base_config,
            max_session_events=2,
            max_active_sessions=1,
            population_max=2,
        )
        generator = EventGenerator(event_dictionary, config)
        stream = TickStreamGenerator(generator)
        tick_at = datetime(2026, 6, 11, 12, 0)

        first_tick = stream.generate_tick(event_budget=5, tick_started_at=tick_at)
        blocked_tick = stream.generate_tick(event_budget=5, tick_started_at=tick_at)
        final_tick = stream.generate_tick(
            event_budget=0,
            tick_started_at=tick_at + timedelta(hours=1),
        )
        later_tick = stream.generate_tick(
            event_budget=0,
            tick_started_at=tick_at + timedelta(hours=2),
        )

        click_id = first_tick["browser_events"][0]["click_id"]

        assert len(first_tick["browser_events"]) == 1
        assert blocked_tick["browser_events"] == []
        assert {event["click_id"] for event in final_tick["browser_events"]} == {click_id}
        assert later_tick["browser_events"] == []

    def test_generate_batch_creates_one_connected_visit(self, event_dictionary, base_config):
        """Публичный вызов генератора создаёт один связанный визит."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(3)

        original_event_ids = {e["event_id"] for e in event_dictionary.browser_events}
        original_click_ids = {e["click_id"] for e in event_dictionary.browser_events}

        browser_events = batch["browser_events"]
        location_events = batch["location_events"]
        device_events = batch["device_events"]
        geo_events = batch["geo_events"]

        click_ids = {event["click_id"] for event in browser_events}
        event_ids = [event["event_id"] for event in browser_events]

        assert len(browser_events) > 1
        assert len(click_ids) == 1
        click_id = next(iter(click_ids))
        assert click_id not in original_click_ids
        uuid.UUID(click_id)

        assert len(set(event_ids)) == len(event_ids)
        assert all(event_id not in original_event_ids for event_id in event_ids)
        for event_id in event_ids:
            uuid.UUID(event_id)

        assert {event["event_id"] for event in location_events} == set(event_ids)
        assert {event["click_id"] for event in device_events} == {click_id}
        assert {event["click_id"] for event in geo_events} == {click_id}

        device_context = [{k: v for k, v in event.items() if k != "click_id"} for event in device_events]
        geo_context = [{k: v for k, v in event.items() if k != "click_id"} for event in geo_events]
        assert len({event["user_domain_id"] for event in device_events}) == 1
        assert all(context == device_context[0] for context in device_context)
        assert all(context == geo_context[0] for context in geo_context)

    def test_generate_batch_creates_multi_event_visit_when_budget_allows(
        self, event_dictionary, base_config
    ):
        """Минимальный связанный визит не схлопывается в одно событие."""
        config = replace(base_config, seed=2)
        generator = EventGenerator(event_dictionary, config)

        batch = generator.generate_batch(3)

        assert len(batch["browser_events"]) >= 2

    def test_event_ids_are_new_uuids(self, event_dictionary, base_config):
        """event_id и click_id — новые UUID, не из оригинальных данных."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(1)

        original_event_ids = {e["event_id"] for e in event_dictionary.browser_events}
        original_click_ids = {e["click_id"] for e in event_dictionary.browser_events}

        browser_event = batch["browser_events"][0]
        assert browser_event["event_id"] not in original_event_ids
        assert browser_event["click_id"] not in original_click_ids

        # Проверяем, что это валидный UUID
        uuid.UUID(browser_event["event_id"])
        uuid.UUID(browser_event["click_id"])

    def test_event_timestamp_format(self, event_dictionary, base_config):
        """event_timestamp имеет правильный формат."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(1)

        browser_event = batch["browser_events"][0]
        timestamp = browser_event["event_timestamp"]

        # Должен парситься как datetime
        dt = datetime.fromisoformat(timestamp.replace(" ", "T"))
        assert dt.year >= 2024

    def test_visit_event_timestamps_strictly_increase(self, event_dictionary, base_config):
        """Время событий внутри одного визита строго возрастает."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(8)

        timestamps = _parse_event_timestamps(batch)

        assert all(
            previous < current
            for previous, current in zip(timestamps, timestamps[1:])
        )

    def test_visit_event_timestamps_use_planned_user_pauses(self, event_dictionary, base_config):
        """Метки времени визита разделены пользовательскими паузами, а не временем цикла."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(8)

        timestamps = _parse_event_timestamps(batch)
        pauses_seconds = [
            (current - previous).total_seconds()
            for previous, current in zip(timestamps, timestamps[1:])
        ]

        assert min(pauses_seconds) >= 1.0

    def test_visit_path_uses_known_funnel_pages(self, event_dictionary, base_config):
        """Путь визита состоит из страниц воронки."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(30)

        page_paths = [event["page_url_path"] for event in batch["location_events"]]

        assert page_paths
        assert set(page_paths) <= ALLOWED_PAGE_PATHS

    def test_visit_starts_from_calibrated_start_distribution(self, event_dictionary, base_config):
        """Визиты стартуют не только с /home, а по стартовому распределению."""
        generator = EventGenerator(event_dictionary, base_config)

        visits = _page_path_visits(generator, 1000)
        first_pages = [visit[0] for visit in visits]
        home_share = first_pages.count("/home") / len(first_pages)
        product_entry_share = (
            first_pages.count("/product_a") + first_pages.count("/product_b")
        ) / len(first_pages)

        assert 0.54 <= home_share <= 0.64
        assert product_entry_share >= 0.25

    def test_visit_length_is_capped_by_session_limit(self, event_dictionary, base_config):
        """Длина визита ограничена потолком, который защищает от петель."""
        config = replace(base_config, max_session_events=4)
        generator = EventGenerator(event_dictionary, config)

        visits = _page_path_visits(generator, 200)

        assert max(len(visit) for visit in visits) <= 4
        assert any(len(visit) == 4 for visit in visits)

    def test_visit_pauses_stay_below_session_timeout_scale(self, event_dictionary, base_config):
        """Паузы внутри визита остаются меньше 30 минут, p95 — единицы минут."""
        generator = EventGenerator(event_dictionary, base_config)
        pauses_seconds = []

        for _ in range(500):
            timestamps = _parse_event_timestamps(generator.generate_batch(30))
            pauses_seconds.extend(
                (current - previous).total_seconds()
                for previous, current in zip(timestamps, timestamps[1:])
            )

        pauses_seconds.sort()
        p95 = pauses_seconds[int(len(pauses_seconds) * 0.95)]

        assert max(pauses_seconds) < 30 * 60
        assert p95 < 5 * 60

    def test_confirmation_share_matches_seed_scale(self, event_dictionary, base_config):
        """Около четверти визитов доходят до /confirmation."""
        generator = EventGenerator(event_dictionary, base_config)

        visits = _page_path_visits(generator, 1000)
        confirmation_share = (
            sum("/confirmation" in visit for visit in visits) / len(visits)
        )

        assert 0.20 <= confirmation_share <= 0.30

    def test_visit_can_continue_after_confirmation(self, event_dictionary, base_config):
        """/confirmation не обязан быть последним событием визита."""
        generator = EventGenerator(event_dictionary, base_config)

        visits = _page_path_visits(generator, 1000)

        assert any(
            page_path == "/confirmation" and index < len(visit) - 1
            for visit in visits
            for index, page_path in enumerate(visit)
        )

    def test_links_consistency(self, event_dictionary, base_config):
        """Связи между событиями сохраняются."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(5)

        for i, browser in enumerate(batch["browser_events"]):
            event_id = browser["event_id"]
            click_id = browser["click_id"]

            # Location должен иметь тот же event_id
            assert batch["location_events"][i]["event_id"] == event_id

            # Device и Geo должны иметь тот же click_id
            assert batch["device_events"][i]["click_id"] == click_id
            assert batch["geo_events"][i]["click_id"] == click_id

    def test_required_fields_present(self, event_dictionary, base_config):
        """Все обязательные поля присутствуют в событиях."""
        generator = EventGenerator(event_dictionary, base_config)
        batch = generator.generate_batch(1)

        browser = batch["browser_events"][0]
        required_fields = [
            "event_id", "event_timestamp", "event_type", "click_id",
            "browser_name", "browser_user_agent", "browser_language"
        ]

        for field in required_fields:
            assert field in browser, f"Missing field: {field}"


class TestPoissonDistribution:
    """Тесты статистической модели."""

    def test_calculate_events_respects_bounds(self, event_dictionary, base_config):
        """Расчет количества событий уважает границы."""
        generator = EventGenerator(event_dictionary, base_config)

        samples = [generator._calculate_events_count() for _ in range(100)]

        assert all(s >= base_config.min_events_per_tick for s in samples)
        assert all(s <= base_config.max_events_per_tick for s in samples)

    def test_jitter_increases_variance(self, event_dictionary, base_config, config_no_jitter):
        """Jitter увеличивает дисперсию."""
        gen_with = EventGenerator(event_dictionary, base_config)
        gen_without = EventGenerator(event_dictionary, config_no_jitter)

        samples_with = [gen_with._calculate_events_count() for _ in range(200)]
        samples_without = [gen_without._calculate_events_count() for _ in range(200)]

        mean_with = sum(samples_with) / len(samples_with)
        mean_without = sum(samples_without) / len(samples_without)

        var_with = sum((x - mean_with) ** 2 for x in samples_with) / len(samples_with)
        var_without = sum((x - mean_without) ** 2 for x in samples_without) / len(samples_without)

        assert var_with > var_without, \
            f"Jitter should increase variance: {var_with} vs {var_without}"

    def test_mean_is_reasonable(self, event_dictionary, base_config):
        """Среднее значение в разумных пределах."""
        generator = EventGenerator(event_dictionary, base_config)

        samples = [generator._calculate_events_count() for _ in range(500)]
        mean = sum(samples) / len(samples)

        # Ожидаем: lambda_base * tick_seconds / 60 * hour_factor
        # hour_factor обычно 0.7-1.2
        expected_base = base_config.lambda_base_per_min * base_config.tick_seconds / 60.0

        # Допустимое отклонение до 50%
        assert mean > expected_base * 0.5, f"Mean {mean} too low (expected ~{expected_base})"
        assert mean < expected_base * 1.5, f"Mean {mean} too high (expected ~{expected_base})"


class TestEmptyData:
    """Тесты обработки пустых данных."""

    def test_empty_jsonl_raises_error(self, empty_temp_dir):
        """Пустые JSONL файлы вызывают ValueError при загрузке."""
        with pytest.raises(ValueError, match="browser_events.jsonl is empty"):
            EventDictionary.load(empty_temp_dir)
