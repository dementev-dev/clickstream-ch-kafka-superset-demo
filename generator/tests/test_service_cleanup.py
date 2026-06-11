"""
Контракт уборки сервиса генератора.
"""

from pathlib import Path
import subprocess
import sys

from generator import (
    Config,
    EventDictionary,
    EventGenerator,
    GeneratorService,
    KafkaPublisher,
    GeneratorState,
)


def test_dockerfile_copies_split_python_modules():
    """Контейнерный запуск видит все модули генератора после разбиения."""
    dockerfile = (Path(__file__).parent.parent / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY src ./src" in dockerfile
    assert 'ENV PYTHONPATH="/app/src"' in dockerfile


def test_generator_facade_imports_without_pythonpath():
    """Локальный фасад сам находит src-пакет без внешнего PYTHONPATH."""
    generator_dir = Path(__file__).parent.parent

    result = subprocess.run(
        [sys.executable, "-c", "import generator"],
        cwd=generator_dir,
        env={"PATH": ""},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_generator_facade_preserves_public_imports_after_split():
    """Старый импорт из generator работает, но классы живут в отдельных модулях."""
    assert Config.__module__ == "clickstream_generator.config"
    assert EventDictionary.__module__ == "clickstream_generator.dictionary"
    assert EventGenerator.__module__ == "clickstream_generator.generation"
    assert KafkaPublisher.__module__ == "clickstream_generator.kafka_io"
    assert GeneratorState.__module__ == "clickstream_generator.state"
    assert GeneratorService.__module__ == "clickstream_generator.service"


def test_tick_batch_is_not_part_of_pure_visit_generation():
    """Временная сборка тика вынесена из чистой модели одного визита."""
    assert not hasattr(EventGenerator, "generate_tick_batch")
