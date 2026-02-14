# Commit Rules

Unified commit style for all project contributors. Follows [Conventional Commits](https://www.conventionalcommits.org/) specification.

## Language

- **Primary language**: Russian
- If language is not specified, use Russian
- For AI-generated commits, Russian is mandatory unless task explicitly sets `lang:en`
- English is allowed only by explicit instruction (`lang:en`) or external collaboration requirements
- Do not mix languages in free-text parts of one commit message (subject + body + footer)
- Conventional Commit `type(scope)` stays in English
- Technical terms (Airflow, ClickHouse, Kafka, MV, DDL) keep as-is

## Header Format

```
<type>(<scope>): <short description>
```

- Maximum header length: 72 characters
- For Russian subject, use result form (e.g. "добавлено", "исправлено", "обновлено")
- For English subject, use imperative present form (e.g. "add", "fix", "update")
- For English subject, do not use past forms (e.g. "added", "fixed", "updated")
- No trailing period
- Keep subject specific; avoid vague messages like "update", "fix bug", "changes"

### Allowed `type`

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `refactor` | Code restructuring without behavior change |
| `docs` | Documentation only |
| `test` | Tests, checks, validations |
| `chore` | Maintenance (configs, scripts, hooks) |
| `ci` | CI/CD changes |
| `perf` | Performance optimization |
| `revert` | Revert previous commit |

### Recommended `scope` for this repo

| Scope | Used for |
|-------|----------|
| `airflow` | DAGs, operators, Airflow config |
| `stg` | STG layer tables and pipelines |
| `ods` | ODS layer tables and pipelines |
| `dds` | DDS layer tables and pipelines |
| `dm` | Data mart views and tables |
| `kafka` | Kafka topics, producers, consumers |
| `superset` | Dashboards, datasets, charts |
| `monitoring` | Prometheus, Grafana, alerts |
| `scripts` | Shell scripts, automation |
| `docs` | Documentation, README, plans |
| `infra` | Docker, compose, infrastructure |

## Body Structure

For non-trivial changes, body is required. Use bullet points for readability.

Body is considered required when at least one condition is true:
- behavior or API/contract changed
- migration, rollback risk, or compatibility impact exists
- more than one meaningful file/module changed
- fix is non-obvious from header alone

### Multiline body in CLI (important)

- Do not pass body as one quoted string with `\n` (it will be stored literally).
- Use multiple `-m` flags, or `-F` with heredoc.

Correct:

```bash
git commit \
  -m "feat(monitoring): добавлены правила алертов Grafana" \
  -m "- Зачем:
  - нужны ранние сигналы проблем ClickHouse
- Что:
  - добавлен provisioning-файл алертов по failed queries, memory, parts
  - зафиксирован uid источника Prometheus для стабильной привязки
- Проверка:
  - POST /api/admin/provisioning/alerting/reload
  - GET /api/v1/provisioning/alert-rules"
```

Also correct:

```bash
git commit -F- <<'MSG'
feat(monitoring): добавлены правила алертов Grafana

- Зачем:
  - нужны ранние сигналы проблем ClickHouse
- Что:
  - добавлен provisioning-файл алертов по failed queries, memory, parts
  - зафиксирован uid источника Prometheus для стабильной привязки
- Проверка:
  - POST /api/admin/provisioning/alerting/reload
  - GET /api/v1/provisioning/alert-rules
MSG
```

### Template (Russian - default)

```
<type>(<scope>): <краткое описание результата>

- Зачем:
  - причина изменения
- Что:
  - ключевое изменение 1
  - ключевое изменение 2
- Проверка:
  - как проверено
```

### Template (English - only with `lang:en`)

```
<type>(<scope>): <short action description>

- Why:
  - reason for change
- What:
  - key change 1
  - key change 2
- Check:
  - how verified (command/test/smoke-check)
```

## Commit Scope Rules

- One commit = one logical task
- Don't mix feature changes with large refactoring
- Update docs in the same commit where behavior changes

## Breaking Changes

Use `!` in header for breaking changes:
```
feat(ods)!: change browser_event table contract
```

Add footer:
```
BREAKING CHANGE: column event_type renamed to event_name
```

## Examples

### Good examples

```
feat(superset): добавлен дашборд e-commerce аналитики

- Зачем:
  - нужна визуализация clickstream для бизнеса
- Что:
  - добавлен сервис superset-init в docker-compose
  - добавлены скрипты подключения к ClickHouse
  - добавлены 10 графиков (KPI, traffic, geo, UTM, DQ)
  - добавлены команды superset-* в Makefile
- Проверка:
  - дашборд открывается на http://localhost:8088
  - все графики загружают данные из dm.v_events_enriched
```

```
fix(kafka): исправлен путь volume для режима KRaft

- Зачем:
  - Kafka не стартует из-за permission denied на /tmp/kraft-combined-logs
- Что:
  - путь volume изменен на /var/lib/kafka/data
- Проверка:
  - `make up` поднимает Kafka без ошибок
```

```
docs(architecture): обновлена схема потоков данных после миграции ODS
```

```
chore(scripts): синхронизирован make transform с новым ETL пайплайном
```

### Bad examples (don't do this)

```
❌ added superset dashboard        # no type, past tense
❌ feat: добавлен дашборд          # no scope
❌ fix: исправлен баг              # no scope, vague and non-actionable
❌ feat(ui): added new filters     # past tense in English subject
❌ feat(airflow): add feature and fix bug and update docs  # multiple concerns
❌ feat(dm): add витрину и почини alert # mixed languages in one message
```

## Quick Reference

```bash
# Feature
feat(scope): добавлена новая возможность

# Bug fix
fix(scope): исправлена проблема

# Documentation
docs(scope): обновлена документация

# Refactoring
refactor(scope): упрощена структура без изменения поведения

# Performance
perf(scope): ускорено выполнение

# Maintenance
chore(scope): обновлены служебные настройки

# Feature (lang:en)
feat(scope): add new capability

# Bug fix (lang:en)
fix(scope): correct response parsing

# Documentation (lang:en)
docs(scope): update setup guide
```
