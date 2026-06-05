"""Общие helper-функции для чтения SQL-файлов из Airflow DAG."""

from __future__ import annotations

from pathlib import Path


def _sql_error(message: str) -> Exception:
    try:
        from airflow.exceptions import AirflowException

        return AirflowException(message)
    except ModuleNotFoundError:
        return ValueError(message)


def strip_sql_comments(sql_text: str) -> str:
    """Удаляет SQL-комментарии, не трогая строки в кавычках."""
    result: list[str] = []
    i = 0
    in_single_quote = False
    in_double_quote = False

    while i < len(sql_text):
        char = sql_text[i]
        next_char = sql_text[i + 1] if i + 1 < len(sql_text) else ""

        if in_single_quote:
            result.append(char)
            if char == "'" and next_char == "'":
                result.append(next_char)
                i += 2
                continue
            if char == "'" and (i == 0 or sql_text[i - 1] != "\\"):
                in_single_quote = False
            i += 1
            continue

        if in_double_quote:
            result.append(char)
            if char == '"' and (i == 0 or sql_text[i - 1] != "\\"):
                in_double_quote = False
            i += 1
            continue

        if char == "'":
            in_single_quote = True
            result.append(char)
            i += 1
            continue

        if char == '"':
            in_double_quote = True
            result.append(char)
            i += 1
            continue

        if char == "-" and next_char == "-":
            i += 2
            while i < len(sql_text) and sql_text[i] not in "\r\n":
                i += 1
            continue

        if char == "/" and next_char == "*":
            i += 2
            while (
                i < len(sql_text) - 1
                and not (sql_text[i] == "*" and sql_text[i + 1] == "/")
            ):
                if sql_text[i] in "\r\n":
                    result.append(sql_text[i])
                i += 1
            i += 2
            continue

        result.append(char)
        i += 1

    return "".join(result)


def split_sql_statements(sql_text: str) -> tuple[str, ...]:
    """Делит SQL на команды по ';' вне строковых литералов."""
    statements: list[str] = []
    current: list[str] = []
    cleaned_sql = strip_sql_comments(sql_text)
    in_single_quote = False
    in_double_quote = False
    i = 0

    while i < len(cleaned_sql):
        char = cleaned_sql[i]
        next_char = cleaned_sql[i + 1] if i + 1 < len(cleaned_sql) else ""

        if in_single_quote:
            current.append(char)
            if char == "'" and next_char == "'":
                current.append(next_char)
                i += 2
                continue
            if char == "'" and (i == 0 or cleaned_sql[i - 1] != "\\"):
                in_single_quote = False
            i += 1
            continue

        if in_double_quote:
            current.append(char)
            if char == '"' and (i == 0 or cleaned_sql[i - 1] != "\\"):
                in_double_quote = False
            i += 1
            continue

        if char == "'":
            in_single_quote = True
            current.append(char)
            i += 1
            continue

        if char == '"':
            in_double_quote = True
            current.append(char)
            i += 1
            continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)

    return tuple(statements)


def load_sql_statements(sql_root: Path, relative_path: str) -> tuple[str, ...]:
    """Читает SQL-файл и возвращает отдельные команды."""
    file_path = sql_root / relative_path
    if not file_path.is_file():
        raise _sql_error(f"SQL-файл не найден: {file_path}")

    statements = split_sql_statements(file_path.read_text(encoding="utf-8"))
    if not statements:
        raise _sql_error(f"SQL-файл пустой: {file_path}")

    return statements
