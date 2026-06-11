# Handoff: генератор разбит на AFK-задачи

Дата: 2026-06-11
Ветка: `feature/data-generator`
Жанр: одноразовые леса по [ADR-0003](../../docs/adr/0003-handoffs-in-scratch.md).

## Где остановились

После обсуждения с пользователем дизайн переработки steady-stream генератора
переведён из больших спек в локальные задачи для агентов. Код генератора не
меняли.

Создан каталог задач:

- `.scratch/feature-data-generator/issues/01-minimal-connected-visit.md`
- `.scratch/feature-data-generator/issues/02-visit-page-path-and-monotonic-time.md`
- `.scratch/feature-data-generator/issues/03-tick-stream-with-active-visits.md`
- `.scratch/feature-data-generator/issues/04-user-population-and-returns.md`
- `.scratch/feature-data-generator/issues/05-intensity-and-flow-calibration.md`
- `.scratch/feature-data-generator/issues/06-state-v2-and-restart.md`
- `.scratch/feature-data-generator/issues/07-service-integration-and-docs.md`

Все задачи имеют `Status: ready-for-agent`. Разрез сделан как последовательные
вертикальные срезы, а не как слои архитектуры. Решение пользователя: идти по
этому варианту.

## Важный контекст

- Старый генератор считаем слабым прототипом, а не ценным кодом для сохранения.
  Сохранять надо внешние контракты, если они полезны: топики Kafka, формат
  сообщений для текущего ETL, команды запуска, метрики, идею compact-топика
  состояния.
- Отдельную задачу "зафиксировать внешний контракт" не создавали: контракт
  встроен в первый и последний срезы.
- Детали модели не дублировать отсюда. Источники истины:
  - `docs/specs/2026-06-10-generator-math-model.md`
  - `docs/specs/2026-06-09-generator-rework-hierarchical.md`
  - `docs/adr/0004-steady-stream-synthetic-generator.md`
  - `CONTEXT.md`
  - `generator/KNOWN_ISSUES.md`
- Предыдущий handoff `.scratch/handoffs/2026-06-10-generator-spec-to-codex.md`
  остаётся полезным как предыстория спек.

## Следующий шаг

Начать с задачи 01 через TDD:

1. написать один красный тест на публичное поведение "минимальный связанный
   визит";
2. реализовать минимальное новое генеративное ядро без Kafka;
3. не тащить старую плоскую модель `generate_batch()`;
4. после зелёного теста переходить к следующему поведению, не писать все тесты
   заранее.

Пример запуска задачи:

```text
/goal Реализовать .scratch/feature-data-generator/issues/01-minimal-connected-visit.md с использованием /tdd. Соблюдать цикл: один тест на наблюдаемое поведение → минимальная реализация → зелёный тест → остановиться и отчитаться. Не писать все тесты заранее, не реализовывать задачи 02-07.
```

## Suggested skills

- `tdd` — для реализации каждой задачи короткими циклами red-green-refactor.
- `adversarial-review` или `claude-team-review` — после реализации нескольких
  срезов, чтобы сверить код с мат-спекой.
- `conventional-commits` — при фиксации следующих изменений.
