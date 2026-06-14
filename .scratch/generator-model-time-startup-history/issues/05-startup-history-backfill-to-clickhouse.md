Status: ready-for-human

# Стартовая история до ClickHouse

## Parent

`.scratch/generator-model-time-startup-history/PRD.md`

## What to build

Сделать промотку прошлого: генератор быстро проходит от `T0` до `T_end`, создаёт
события за этот отрезок, сохраняет слепок состояния и манифест стартовой
истории. Эту историю нужно загрузить в ClickHouse и доказать, что живой поток
может продолжить её с `T_end`.

Это главный срез стартовой истории. Он должен проверять не только факт наличия
данных, но и форму данных: пирамиду, возвраты, длину визита, воронку и стык
между прошлым и живым продолжением.

## Acceptance criteria

- [x] Промотка прошлого создаёт события за `[T0, T_end)`, слепок состояния и
  манифест с контрольными данными.
- [x] При одинаковых `GEN_SEED`, `T0`, `T_end` и настройках артефакт промотки
  прошлого повторяем точно; проверки в ClickHouse следуют правилам допуска из
  контракта задачи 1.
- [x] История загружается в ClickHouse штатной или явно описанной командой.
- [x] SQL-проверка показывает здоровую пирамиду: пользователей меньше, чем
  визитов, визитов меньше, чем событий.
- [x] SQL-проверка показывает возвраты: у части пользователей больше одного
  визита.
- [x] SQL-проверка длины визита проверяет форму, а не только среднее: долю
  коротких визитов, медиану и долю срезов о потолок.
- [x] SQL-проверка воронки `/home -> товары -> /cart -> /payment ->
  /confirmation` монотонно убывает, а доля дошедших до `/confirmation` в
  согласованном коридоре.
- [x] Живое продолжение после `T_end` не создаёт дублей на границе и не выглядит
  как независимый второй мир.
- [x] Визиты, которые переходят через `T_end`, остаются однородными: контекст
  визита не меняется на стыке стартовой истории и живого продолжения.
- [x] Подготовлены данные, команды и SQL-проверки, достаточные для внешнего
  review gate по распределениям и двум путям генерации из `PRD.md`.
- [x] Worker даёт промежуточный статус, если промотка, загрузка в ClickHouse или
  распределительные проверки занимают заметное время.

## Blocked by

- `.scratch/generator-model-time-startup-history/issues/04-state-v2-model-resume.md`

## Фактический прогон worker-а

Дата проверки: 2026-06-14.

Настройки стенда:

- `GEN_SEED=4242`
- `GEN_MODEL_T0=2026-01-01T00:00:00+00:00`
- `GEN_MODEL_T_END=2026-01-01T06:00:00+00:00`
- `GEN_MODEL_TIMEZONE=UTC`
- `GEN_MODEL_TIME_SPEED=1`
- `GEN_TICK_SECONDS=60`
- `GEN_LAMBDA_BASE_PER_MIN=120`
- `GEN_JITTER_PCT=0`
- `GEN_MIN_EVENTS_PER_TICK=1`
- `GEN_MAX_EVENTS_PER_TICK=1000`

Команды проверки:

```bash
make clean
docker compose up -d clickhouse kafka
make ddl
docker compose build generator

docker compose run --rm --no-deps \
  -e GEN_RUN_MODE=backfill \
  -e GEN_STATE_RESET=true \
  -e GEN_SEED=4242 \
  -e GEN_MODEL_T0=2026-01-01T00:00:00+00:00 \
  -e GEN_MODEL_T_END=2026-01-01T06:00:00+00:00 \
  -e GEN_MODEL_TIMEZONE=UTC \
  -e GEN_MODEL_TIME_SPEED=1 \
  -e GEN_TICK_SECONDS=60 \
  -e GEN_LAMBDA_BASE_PER_MIN=120 \
  -e GEN_JITTER_PCT=0 \
  -e GEN_MIN_EVENTS_PER_TICK=1 \
  -e GEN_MAX_EVENTS_PER_TICK=1000 \
  generator

sleep 10
bash scripts/run_batch.sh
```

Повторяемость проверена двумя чистыми прогонами с полным сбросом
ClickHouse/Kafka/state через `make clean`. В обоих прогонах manifest и ClickHouse
дали одинаковые контрольные числа:

- manifest `browser_events.checksum_sha256`:
  `3b1946bd7fd3d669e2461fcbae88d8aed6980bb5f126779fb4e7480ce15a3269`
- manifest `location_events.checksum_sha256`:
  `370816c8d0b0311592282b9b75372e18862db9ef4984f7032885e935f9436522`
- manifest `device_events.checksum_sha256`:
  `37e157ac7d5a3203c1c27b2a11abc1b9cb9fc7f74f79c77645faa09412e08b33`
- manifest `geo_events.checksum_sha256`:
  `ec0e36e4613e05b9a5e72490c7565cf7e5f767d39a7a58b383e56e2eb191fc20`
- ClickHouse digest:
  `CC1A73E65E897D1F1FC982CFA4237A07`

Backfill в ClickHouse:

- `events=31825`
- `unique_events=31825`
- `visits=3020`
- `users=1048`
- `min_event_ts=2026-01-01 00:00:00.000000`
- `max_event_ts=2026-01-01 05:59:59.521599`
- `pyramid_ok=1`
- `half_open_ok=1`

Проверка возвратов:

- `users=1048`
- `returning_users=708`
- `returning_share=0.6755725190839694`
- `max_visits_per_user=11`

Форма длины визита:

- `visits=3020`
- `short_visit_share=0.16490066225165562`
- `median_events_per_visit=8`
- `avg_events_per_visit=10.538079470198676`
- `capped_visit_share=0.0619205298013245`
- `median_duration_sec=202`
- `p95_duration_sec=823`
- `max_events_per_visit=30`

Воронка:

- строгая упорядоченная:
  `home=2743`, `products=1626`, `cart=995`, `payment=673`,
  `confirmation=394`, `monotonic_ok=1`,
  `ordered_confirmation_share=0.14363835216915785`;
- калибровочная по наличию страницы в визите:
  `home=2743`, `products=2706`, `cart=2003`, `payment=1368`,
  `confirmation=824`, `monotonic_ok=1`,
  `confirmation_share_all_visits=0.2728476821192053`,
  `confirmation_share_from_home=0.3004010207801677`.

Live-продолжение:

```bash
docker compose run -d --name issue05-live --no-deps \
  -e GEN_RUN_MODE=live \
  -e GEN_STATE_RESET=false \
  -e GEN_SEED=4242 \
  -e GEN_MODEL_T0=2026-01-01T00:00:00+00:00 \
  -e GEN_MODEL_T_END=2026-01-01T06:00:00+00:00 \
  -e GEN_MODEL_TIMEZONE=UTC \
  -e GEN_MODEL_TIME_SPEED=1 \
  -e GEN_TICK_SECONDS=60 \
  -e GEN_LAMBDA_BASE_PER_MIN=120 \
  -e GEN_JITTER_PCT=0 \
  -e GEN_MIN_EVENTS_PER_TICK=1 \
  -e GEN_MAX_EVENTS_PER_TICK=1000 \
  generator
```

Лог live-запуска подтвердил восстановление из стартовой истории:
`model_time=2026-01-01T06:00:00+00:00`, затем прошли тики `361`-`364`.

После live и повторного `bash scripts/run_batch.sh`:

- `users=1079`
- `visits=3069`
- `events=32145`
- `min_event_ts=2026-01-01 00:00:00.000000`
- `max_event_ts=2026-01-01 06:03:00.000000`
- `history_events=31825`
- `live_events=320`
- `duplicate_events=0`
- `boundary_events=13`

`boundary_events=13` — это live-события ровно на `T_end`; они не входили в
backfill `[T0, T_end)`.

Визиты через `T_end`:

- `crossing_visits=36`
- `visits_with_both_sides=36`
- `homogeneous_visits=36`
- `context_ok=1`
- `min_before_events=2`
- `min_after_events=1`
- `max_after_events=10`

## Риски для review gate

- Manifest, state и события связаны метаданными (`GEN_SEED`, `T0`, `T_end`,
  настройки генерации, `last_batch_id`) и checksum событий по топикам. Полной
  криптографической связки `state + events + manifest` пока нет.
- Для воронки есть две SQL-формы. Строгая ordered-форма проверяет порядок
  `/home -> товары -> /cart -> /payment -> /confirmation`. Калибровочная
  contains-форма проверяет, была ли страница в визите. Для review gate
  `confirmation_share` сравнивается с коридором мат-модели по contains-форме.

## Review gate

- Саморевью worker-а нашло блокирующий риск: backfill не должен сохранять
  state/manifest при частичной публикации. Исправлено fail-fast поведением и
  тестом.
- Первый reviewer gate нашёл два блокера: startup-history state мог остаться без
  manifest и восстановиться как live-state, а startup-history restore не сверял
  поля самого state. Оба пункта исправлены до коммита.
- Повторный reviewer gate: `gate pass`, новых блокирующих находок нет.
- Проверки после исправлений: `make generator-test` — 134 passed.
- Остаточный риск процесса: reviewer был со свежим контекстом, но той же
  родословной, а не внешней моделью.
