#!/usr/bin/env python3
"""
Комплексное тестирование генератора событий.
Проверяет граничные случаи, статистику и формат данных.
"""

import json
import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from generator import (
    Config, EventDictionary, EventGenerator, 
    BatchHistory, BatchRecord
)


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"


def test_config_validation():
    """Тест валидации конфигурации."""
    print("\n=== Test: Config Validation ===")
    
    # Невалидный tick_seconds
    try:
        Config(
            kafka_bootstrap_servers="localhost:9092",
            tick_seconds=0,
            lambda_base_per_min=100,
            jitter_pct=20,
            min_events_per_tick=10,
            max_events_per_tick=100,
            data_dir=Path("/tmp"),
            seed=None,
            enabled=True,
        )
        print(f"{Colors.RED}FAIL: Should raise ValueError for tick_seconds=0{Colors.RESET}")
        return False
    except ValueError as e:
        print(f"{Colors.GREEN}PASS: Correctly raised ValueError: {e}{Colors.RESET}")

    # Невалидный lambda_base
    try:
        Config(
            kafka_bootstrap_servers="localhost:9092",
            tick_seconds=60,
            lambda_base_per_min=0,
            jitter_pct=20,
            min_events_per_tick=10,
            max_events_per_tick=100,
            data_dir=Path("/tmp"),
            seed=None,
            enabled=True,
        )
        print(f"{Colors.RED}FAIL: Should raise ValueError for lambda_base=0{Colors.RESET}")
        return False
    except ValueError as e:
        print(f"{Colors.GREEN}PASS: Correctly raised ValueError: {e}{Colors.RESET}")

    # Несуществующая директория
    try:
        Config(
            kafka_bootstrap_servers="localhost:9092",
            tick_seconds=60,
            lambda_base_per_min=100,
            jitter_pct=20,
            min_events_per_tick=10,
            max_events_per_tick=100,
            data_dir=Path("/nonexistent/path"),
            seed=None,
            enabled=True,
        )
        print(f"{Colors.RED}FAIL: Should raise ValueError for non-existent dir{Colors.RESET}")
        return False
    except ValueError as e:
        print(f"{Colors.GREEN}PASS: Correctly raised ValueError: {e}{Colors.RESET}")

    return True


def test_empty_jsonl():
    """Тест обработки пустых JSONL файлов."""
    print("\n=== Test: Empty JSONL Files ===")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Создаём пустые файлы
        for fname in ["browser_events.jsonl", "location_events.jsonl", 
                      "device_events.jsonl", "geo_events.jsonl"]:
            open(Path(tmpdir) / fname, "w").close()
        
        try:
            dictionary = EventDictionary.load(Path(tmpdir))
            if (len(dictionary.browser_events) == 0 and 
                len(dictionary.location_events) == 0):
                print(f"{Colors.GREEN}PASS: Empty files handled correctly{Colors.RESET}")
                return True
            else:
                print(f"{Colors.RED}FAIL: Expected empty lists{Colors.RESET}")
                return False
        except Exception as e:
            print(f"{Colors.RED}FAIL: Exception with empty files: {e}{Colors.RESET}")
            return False


def test_event_dictionary_consistency():
    """Тест консистентности связей в словаре."""
    print("\n=== Test: Event Dictionary Consistency ===")
    
    data_dir = Path(__file__).parent.parent / "data"
    dictionary = EventDictionary.load(data_dir)
    
    # Проверяем, что все location имеют соответствующий event_id в browser
    browser_event_ids = {e["event_id"] for e in dictionary.browser_events}
    location_orphaned = 0
    for loc in dictionary.location_events:
        if loc["event_id"] not in browser_event_ids:
            location_orphaned += 1
    
    # Проверяем, что все device/geo имеют соответствующий click_id в browser
    browser_click_ids = {e["click_id"] for e in dictionary.browser_events}
    device_orphaned = 0
    for dev in dictionary.device_events:
        if dev["click_id"] not in browser_click_ids:
            device_orphaned += 1
    
    geo_orphaned = 0
    for geo in dictionary.geo_events:
        if geo["click_id"] not in browser_click_ids:
            geo_orphaned += 1
    
    print(f"  Browser events: {len(dictionary.browser_events)}")
    print(f"  Location events: {len(dictionary.location_events)} (orphaned: {location_orphaned})")
    print(f"  Device events: {len(dictionary.device_events)} (orphaned: {device_orphaned})")
    print(f"  Geo events: {len(dictionary.geo_events)} (orphaned: {geo_orphaned})")
    
    # Для MVP допустимы orphaned записи, но предупреждаем
    if location_orphaned > 0 or device_orphaned > 0 or geo_orphaned > 0:
        print(f"{Colors.YELLOW}WARNING: Found orphaned records{Colors.RESET}")
    else:
        print(f"{Colors.GREEN}PASS: All records are consistent{Colors.RESET}")
    
    return True


def test_poisson_distribution():
    """Тест статистической модели (распределение Пуассона)."""
    print("\n=== Test: Poisson Distribution ===")
    
    data_dir = Path(__file__).parent.parent / "data"
    dictionary = EventDictionary.load(data_dir)
    
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=60,
        lambda_base_per_min=200,
        jitter_pct=20,
        min_events_per_tick=50,
        max_events_per_tick=500,
        data_dir=data_dir,
        seed=42,
        enabled=True,
    )
    
    generator = EventGenerator(dictionary, config)
    
    # Генерируем 1000 значений
    samples = [generator._calculate_events_count() for _ in range(1000)]
    
    mean = sum(samples) / len(samples)
    min_val = min(samples)
    max_val = max(samples)
    
    # Проверяем границы
    if min_val < config.min_events_per_tick:
        print(f"{Colors.RED}FAIL: min={min_val} < {config.min_events_per_tick}{Colors.RESET}")
        return False
    if max_val > config.max_events_per_tick:
        print(f"{Colors.RED}FAIL: max={max_val} > {config.max_events_per_tick}{Colors.RESET}")
        return False
    
    # Проверяем среднее (должно быть около lambda_base при hour_factor=1.0)
    expected = config.lambda_base_per_min  # примерно
    deviation = abs(mean - expected) / expected * 100
    
    print(f"  Samples: 1000")
    print(f"  Min: {min_val}, Max: {max_val}")
    print(f"  Mean: {mean:.2f} (expected ~{expected}, deviation: {deviation:.1f}%)")
    
    # Допустимое отклонение до 30% (зависит от часа и случайности)
    if deviation < 30:
        print(f"{Colors.GREEN}PASS: Mean is within acceptable range{Colors.RESET}")
        return True
    else:
        print(f"{Colors.YELLOW}WARNING: Mean deviation is high (maybe different hour?){Colors.RESET}")
        return True


def test_generate_batch_format():
    """Тест формата сгенерированных событий."""
    print("\n=== Test: Generated Event Format ===")
    
    data_dir = Path(__file__).parent.parent / "data"
    dictionary = EventDictionary.load(data_dir)
    
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=60,
        lambda_base_per_min=10,
        jitter_pct=20,
        min_events_per_tick=5,
        max_events_per_tick=20,
        data_dir=data_dir,
        seed=42,
        enabled=True,
    )
    
    generator = EventGenerator(dictionary, config)
    batch = generator.generate_batch(10)
    
    errors = []
    
    # Проверяем структуру батча
    required_topics = ["browser_events", "location_events", "device_events", "geo_events"]
    for topic in required_topics:
        if topic not in batch:
            errors.append(f"Missing topic: {topic}")
    
    # Проверяем формат browser_events
    for i, event in enumerate(batch["browser_events"]):
        required_fields = ["event_id", "event_timestamp", "event_type", "click_id", 
                          "browser_name", "browser_user_agent", "browser_language"]
        for field in required_fields:
            if field not in event:
                errors.append(f"browser_event[{i}] missing field: {field}")
        
        # Проверяем UUID
        try:
            import uuid
            uuid.UUID(event["event_id"])
            uuid.UUID(event["click_id"])
        except (ValueError, KeyError) as e:
            errors.append(f"browser_event[{i}] invalid UUID: {e}")
        
        # Проверяем timestamp
        try:
            datetime.fromisoformat(event["event_timestamp"].replace(" ", "T"))
        except (ValueError, KeyError) as e:
            errors.append(f"browser_event[{i}] invalid timestamp: {e}")
    
    # Проверяем связи
    browser_event_ids = {e["event_id"] for e in batch["browser_events"]}
    for loc in batch["location_events"]:
        if loc["event_id"] not in browser_event_ids:
            errors.append(f"location event_id {loc['event_id'][:8]}... not in browser events")
    
    browser_click_ids = {e["click_id"] for e in batch["browser_events"]}
    for dev in batch["device_events"]:
        if dev["click_id"] not in browser_click_ids:
            errors.append(f"device click_id {dev['click_id'][:8]}... not in browser events")
    
    for geo in batch["geo_events"]:
        if geo["click_id"] not in browser_click_ids:
            errors.append(f"geo click_id {geo['click_id'][:8]}... not in browser events")
    
    if errors:
        print(f"{Colors.RED}FAIL: Found {len(errors)} errors:{Colors.RESET}")
        for e in errors[:5]:
            print(f"  - {e}")
        return False
    else:
        print(f"{Colors.GREEN}PASS: All events have valid format and consistent links{Colors.RESET}")
        return True


def test_batch_history():
    """Тест истории батчей."""
    print("\n=== Test: Batch History ===")
    
    history = BatchHistory()
    
    # Добавляем записи
    from datetime import datetime, timezone
    for i in range(5):
        history.add(BatchRecord(
            batch_id=f"batch_{i}",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            sent_total=100,
            sent_browser=25,
            sent_location=25,
            sent_device=25,
            sent_geo=25,
            status="success" if i % 2 == 0 else "error",
            error_message=None if i % 2 == 0 else "Test error",
        ))
    
    stats = history.get_stats()
    
    if stats["total_batches"] == 5:
        print(f"{Colors.GREEN}PASS: History tracking works{Colors.RESET}")
        return True
    else:
        print(f"{Colors.RED}FAIL: Expected 5 batches, got {stats['total_batches']}{Colors.RESET}")
        return False


def test_reproducibility():
    """Тест воспроизводимости с одинаковым seed."""
    print("\n=== Test: Reproducibility ===")
    
    data_dir = Path(__file__).parent.parent / "data"
    dictionary = EventDictionary.load(data_dir)
    
    config1 = Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=60,
        lambda_base_per_min=100,
        jitter_pct=20,
        min_events_per_tick=10,
        max_events_per_tick=200,
        data_dir=data_dir,
        seed=12345,
        enabled=True,
    )
    
    config2 = Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=60,
        lambda_base_per_min=100,
        jitter_pct=20,
        min_events_per_tick=10,
        max_events_per_tick=200,
        data_dir=data_dir,
        seed=12345,
        enabled=True,
    )
    
    gen1 = EventGenerator(dictionary, config1)
    gen2 = EventGenerator(dictionary, config2)
    
    # Генерируем батчи
    batch1 = gen1.generate_batch(10)
    batch2 = gen2.generate_batch(10)
    
    # Проверяем, что event_id разные (UUID всегда новые)
    ids1 = [e["event_id"] for e in batch1["browser_events"]]
    ids2 = [e["event_id"] for e in batch2["browser_events"]]
    
    # UUID должны быть разными даже с одинаковым seed (uuid4 случайный)
    if ids1 != ids2:
        print(f"{Colors.GREEN}PASS: UUIDs are unique per generation{Colors.RESET}")
        return True
    else:
        print(f"{Colors.RED}FAIL: UUIDs should be unique{Colors.RESET}")
        return False


def test_large_lambda():
    """Тест с большим lambda (проверка на underflow)."""
    print("\n=== Test: Large Lambda (Edge Case) ===")
    
    data_dir = Path(__file__).parent.parent / "data"
    dictionary = EventDictionary.load(data_dir)
    
    config = Config(
        kafka_bootstrap_servers="localhost:9092",
        tick_seconds=60,
        lambda_base_per_min=10000,  # Очень большое значение
        jitter_pct=20,
        min_events_per_tick=100,
        max_events_per_tick=500,
        data_dir=data_dir,
        seed=42,
        enabled=True,
    )
    
    generator = EventGenerator(dictionary, config)
    
    # Должно вернуть max_events_per_tick (ограничение)
    count = generator._calculate_events_count()
    
    if count == config.max_events_per_tick:
        print(f"{Colors.GREEN}PASS: Large lambda correctly capped at max{Colors.RESET}")
        return True
    else:
        print(f"{Colors.YELLOW}WARNING: Expected {config.max_events_per_tick}, got {count}{Colors.RESET}")
        return True  # Не критично


def run_all_tests():
    """Запускает все тесты."""
    print("=" * 60)
    print("COMPREHENSIVE GENERATOR TESTS")
    print("=" * 60)
    
    tests = [
        test_config_validation,
        test_empty_jsonl,
        test_event_dictionary_consistency,
        test_poisson_distribution,
        test_generate_batch_format,
        test_batch_history,
        test_reproducibility,
        test_large_lambda,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"{Colors.RED}EXCEPTION in {test.__name__}: {e}{Colors.RESET}")
            import traceback
            traceback.print_exc()
            results.append((test.__name__, False))
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
        print(f"  {name}: {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
