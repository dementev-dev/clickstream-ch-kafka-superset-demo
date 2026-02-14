"""
Тесты истории батчей.
"""

from datetime import datetime, timezone

import pytest
from generator import InMemoryBatchHistory, BatchRecord


class TestInMemoryBatchHistory:
    """Тесты in-memory истории."""

    def test_add_record(self):
        """Добавление записи в историю."""
        history = InMemoryBatchHistory()
        record = BatchRecord(
            batch_id="test_1",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            sent_total=100,
            sent_browser=25,
            sent_location=25,
            sent_device=25,
            sent_geo=25,
            status="success",
            error_message=None,
        )
        history.add(record)

        stats = history.get_stats()
        assert stats["total_batches"] == 1
        assert stats["success_rate"] == 1.0
        assert stats["last_batch_status"] == "success"

    def test_history_limits_to_1000(self):
        """История ограничена 1000 записями."""
        history = InMemoryBatchHistory()

        for i in range(1100):
            record = BatchRecord(
                batch_id=f"batch_{i}",
                started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc),
                sent_total=10,
                sent_browser=2,
                sent_location=2,
                sent_device=2,
                sent_geo=2,
                status="success",
                error_message=None,
            )
            history.add(record)

        assert len(history.batches) == 1000
        assert history.batches[0].batch_id == "batch_100"  # Первые 100 удалены

    def test_success_rate_calculation(self):
        """Правильный расчет success rate."""
        history = InMemoryBatchHistory()

        # 3 success, 2 error
        for i in range(5):
            record = BatchRecord(
                batch_id=f"batch_{i}",
                started_at=datetime.now(timezone.utc),
                finished_at=datetime.now(timezone.utc),
                sent_total=100,
                sent_browser=25,
                sent_location=25,
                sent_device=25,
                sent_geo=25,
                status="success" if i < 3 else "error",
                error_message=None if i < 3 else "Test error",
            )
            history.add(record)

        stats = history.get_stats()
        assert stats["total_batches"] == 5
        assert stats["success_rate"] == 0.6  # 3/5

    def test_empty_history_returns_empty_stats(self):
        """Пустая история возвращает пустой dict."""
        history = InMemoryBatchHistory()
        stats = history.get_stats()
        assert stats == {}


class TestBatchRecord:
    """Тесты структуры записи."""

    def test_record_creation(self):
        """Создание записи с всеми полями."""
        now = datetime.now(timezone.utc)
        record = BatchRecord(
            batch_id="abc123",
            started_at=now,
            finished_at=now,
            sent_total=400,
            sent_browser=100,
            sent_location=100,
            sent_device=100,
            sent_geo=100,
            status="partial",
            error_message="Some errors occurred",
        )

        assert record.batch_id == "abc123"
        assert record.sent_total == 400
        assert record.status == "partial"
        assert record.error_message == "Some errors occurred"
