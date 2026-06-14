# Handoff: задачи по модельному времени и стартовой истории

Дата: 2026-06-14
Жанр: одноразовый handoff по ADR-0003.

## Что сделано

Создан рабочий набор артефактов для следующей фазы работ:

- `.scratch/generator-model-time-startup-history/PRD.md`
- `.scratch/generator-model-time-startup-history/issues/01-time-and-startup-history-contract.md`
- `.scratch/generator-model-time-startup-history/issues/02-model-time-to-clickhouse.md`
- `.scratch/generator-model-time-startup-history/issues/03-model-speed-and-day-factor.md`
- `.scratch/generator-model-time-startup-history/issues/04-state-v2-model-resume.md`
- `.scratch/generator-model-time-startup-history/issues/05-startup-history-backfill-to-clickhouse.md`
- `.scratch/generator-model-time-startup-history/issues/06-generated-history-as-analytics-source.md`

Артефакты режут работу на 6 последовательных issue и 2 review gate. Главная
идея: каждый кодовый срез должен подтверждаться через ClickHouse, а не только
локальными тестами генератора. Финальная проверка дашбордов глазами остаётся в
конце.

## Важные решения

- Координатор не берёт `/goal` на всю цепочку: один worker получает один issue.
- Worker работает через `/tdd`, не коммитит и не реализует следующие задачи.
- После реализации worker делает саморевью без правок.
- Координатор классифицирует находки и коммитит сам.
- После задачи 3 нужен review gate по сквозному инварианту времени.
- После задачи 5 нужен review gate по распределениям и двум путям генерации.
- Для этих review gate по возможности нужен reviewer другой родословной: это
  следует из `docs/research/2026-06-11-subagent-coordinator-experiment.md`.

## Что важно не потерять

- В задаче 1 нужно зафиксировать durable-контракт, а не оставить решение только
  в комментарии к issue.
- При ×K восстановление после сбоя нельзя считать простым `datetime.now()`: нужна
  сохранённая связка модельного и настенного времени.
- Стартовая история должна быть парным артефактом: события, слепок состояния и
  манифест.
- Задача 6 специально сужена до штатного пути стенда и документов запуска.
  Уроки, новые панели и улучшения дашбордов уходят в follow-up, если окажутся
  нетривиальными.

## Suggested skills

- `to-issues` — если понадобится переразбить или опубликовать дополнительные
  issue.
- `tdd` — основной режим работы worker-а над каждым кодовым issue.
- `conventional-commits` — перед каждым коммитом координатора.
- `claude-team-review` или другой внешний reviewer — на двух review gate.
- `handoff` — если работа прерывается между issue или после review gate.

## Следующий шаг

Начать с
`.scratch/generator-model-time-startup-history/issues/01-time-and-startup-history-contract.md`.
Это HITL-задача: нужно закрепить интерфейс модельного времени, манифест
стартовой истории, правило границы `T_end` и способ повторяемой проверки в
ClickHouse.
