"""Общие helper-функции для параметров Airflow DAG."""

from __future__ import annotations

TRUE_VALUES = {"1", "true", "t", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "f", "no", "n", "off"}


def _param_error(message: str) -> Exception:
    try:
        from airflow.exceptions import AirflowException

        return AirflowException(message)
    except ModuleNotFoundError:
        return ValueError(message)


def parse_bool_param(value: object, name: str) -> bool:
    """Преобразует bool-параметр из dag_run.conf/params в явный boolean."""
    if isinstance(value, bool):
        return value

    if isinstance(value, int) and value in (0, 1):
        return bool(value)

    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in TRUE_VALUES:
            return True
        if normalized in FALSE_VALUES:
            return False

    raise _param_error(f"Параметр {name} должен быть boolean, получено: {value!r}")
