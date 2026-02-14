"""
Тесты структуры записи батча.
"""

from datetime import datetime, timezone

import pytest
from generator import BatchRecord


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
