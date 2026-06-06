# Handoff: реализация редизайна Superset-дашборда

Дата: 2026-06-06 · Язык сессии: русский · Понять-режим был включён (можно не продолжать)

## Источник истины

**Сначала прочитать спеку:** [`docs/specs/2026-06-06-superset-dashboard-redesign.md`](../../docs/specs/2026-06-06-superset-dashboard-redesign.md)
— там весь дизайн (проблема, числа данных, триаж чартов, состав KPI, решения, риски,
критерии проверки). Этот handoff — только «как возобновить», не дублирует дизайн.
Доменные термины — [`CONTEXT.md`](../../CONTEXT.md).

## Решение одной строкой

KPI-полоса = `Total Events · Unique Users · Avg Events/Visit · Conversion to /confirmation`
(дубль «Unique Sessions» убрать); `Top Pages → Funnel`-чарт по страницам; различие
user/session — текстом, не двумя одинаковыми цифрами; мёртвые колонки purchases — выкинуть.

## Сделать ПЕРВЫМ делом

1. **Снять open questions из спеки** (без них реализация буксует):
   - точная формула Conversion (доля визитов с ≥1 pageview `/confirmation`?);
   - поддерживает ли Superset **4.1.2** `viz_type` воронки (иначе — упорядоченный bar);
   - судьба `Events by Hour` (проверить, не пустой ли на полных данных).
2. **Перегрузить стенд на ПОЛНЫЕ данные** — сейчас в ClickHouse отладочный срез
   (50 событий). Нужно: `make data` (без `LIMIT`) + `make transform`. На полных
   данных: 1000 событий, 99 визитов, 99 пользователей.

## Где править и как прогонять

- Единственный файл реализации: `superset/create_dashboard.py`
  (`CHARTS_CONFIG` / `DASHBOARD_ROWS` / `ROW_HEIGHTS`).
- Прогон: `make superset-dashboard` — идемпотентно (чарты по `slice_name`, дашборд
  по `slug`, обновляются на месте). При переименовании чартов следить, чтобы не
  плодились дубли.

## Проверка

- `GET /api/v1/dashboard/<id>/datasets` → 200; DQ Summary без `Columns missing in datasource`.
- Визуально: `playwright-cli` — логин формой `admin`/`admin` на `http://localhost:8088/login/`,
  затем `goto .../superset/dashboard/ecommerce-analytics/`, `screenshot --filename=/tmp/x.png` (читать через Read).
  Скриншоты — в `/tmp`. `.playwright-cli/` НЕ коммитить.
- Сверка чисел с ClickHouse: креды в `configs/default_user.xml` (default / `123456`),
  `docker exec clickstream-ch-kafka-superset-demo-clickhouse-1 clickhouse-client --password 123456 -q "..."`.
  (Через `docker exec printenv` креды НЕ дёргать — классификатор блокирует.)

## Если задачу берёт Codex (разрез по приёмке)

Codex силён в реализации/рассуждении, **слаб в визуальной оценке** — а приёмка
дашборда визуальна (обе прошлые сессии были про поломки раскладки: ROW-оверлапы,
«прыгающие» KPI). Поэтому:

- Codex делает реализацию + **не**визуальные само-проверки: API `/datasets → 200`,
  числа сходятся с прямым запросом в ClickHouse, скриншоты складывает в `/tmp`.
- Codex **НЕ объявляет done по визуалу.** Самый рискованный пункт (отрисовалась ли
  воронка вообще, нет ли наездов плиток) — чисто визуальный. Остановиться и передать
  скриншоты на визуальный sign-off человеку или vision-агенту.

## Синхронизировать доки ПРИ реализации

- `docs/SUPERSET_DASHBOARD.md` — раздел «Структура дашборда» (новый состав чартов/KPI).
- `docs/course/lessons/06_superset_bi.md` — убрать `LIMIT=50 make data`, синхронизировать состав/скриншоты.

## Git-гигиена

- Ветка `docs/advanced-clickstream-course`, на ней **параллельно пишет Codex** —
  git строго **аддитивно**, не amend/rebase/reset чужих коммитов. `git add` только своих файлов.
- Артефакты этой сессии (если ещё не закоммичены): `CONTEXT.md`, `docs/adr/0001` (правка),
  `docs/adr/0002`, `docs/adr/0003`, `docs/specs/2026-06-06-...`, `AGENTS.md` (правка), этот handoff.

## Suggested skills

`conventional-commits` (любой коммит) · `playwright-cli` (визуальная проверка) ·
`diagnose` (если чарт/датасет отвалится).
