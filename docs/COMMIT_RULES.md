# Commit Rules

Unified commit style for all project contributors. Follows [Conventional Commits](https://www.conventionalcommits.org/) specification.

## Language

- **Primary language**: English
- Russian is allowed for internal team convenience
- Technical terms (Airflow, ClickHouse, Kafka, MV, DDL) keep as-is

## Header Format

```
<type>(<scope>): <short description>
```

- Maximum header length: 72 characters
- Use imperative mood ("add", "fix", "update", not "added", "fixed")
- No trailing period

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

### Multiline body in CLI (important)

- Do not pass body as one quoted string with `\n` (it will be stored literally).
- Use multiple `-m` flags, or `-F` with heredoc.

Correct:

```bash
git commit \
  -m "feat(monitoring): add Grafana alert rules" \
  -m "- Why:
  - need proactive signals for ClickHouse health
- What:
  - add alert provisioning file for failed queries, memory, parts
  - pin Prometheus datasource uid for stable dashboard binding
- Check:
  - POST /api/admin/provisioning/alerting/reload
  - GET /api/v1/provisioning/alert-rules"
```

Also correct:

```bash
git commit -F- <<'MSG'
feat(monitoring): add Grafana alert rules

- Why:
  - need proactive signals for ClickHouse health
- What:
  - add alert provisioning file for failed queries, memory, parts
  - pin Prometheus datasource uid for stable dashboard binding
- Check:
  - POST /api/admin/provisioning/alerting/reload
  - GET /api/v1/provisioning/alert-rules
MSG
```

### Template (English)

```
<type>(<scope>): <short description>

- Why:
  - reason for change
- What:
  - key change 1
  - key change 2
- Check:
  - how verified (command/test/smoke-check)
```

### Template (Russian - допустимо)

```
<type>(<scope>): <краткое описание>

- Зачем:
  - причина изменения
- Что:
  - ключевое изменение 1
  - ключевое изменение 2
- Проверка:
  - как проверено
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
feat(superset): add e-commerce analytics dashboard

- Why:
  - Business needs visualization for clickstream analysis
- What:
  - Add superset-init service to docker-compose
  - Create Python scripts for ClickHouse connection
  - Add 10 charts (KPI, traffic, geo, UTM, DQ)
  - Makefile commands superset-*
- Check:
  - Dashboard opens at http://localhost:8088
  - All charts load data from dm.v_events_enriched
```

```
fix(kafka): correct volume path for KRaft mode

- Why:
  - Kafka fails to start with permission denied on /tmp/kraft-combined-logs
- What:
  - Change volume path to /var/lib/kafka/data
- Check:
  - make up starts Kafka successfully
```

```
docs(architecture): update data flow diagram after ODS migration
```

```
chore(scripts): sync make transform with new ETL pipeline
```

### Bad examples (don't do this)

```
❌ added superset dashboard        # no type, past tense
❌ feat: добавлен дашборд          # no scope, mixed languages
❌ fix: исправлен баг              # no scope, past tense, vague description
❌ feat(airflow): add feature and fix bug and update docs  # multiple concerns
```

## Quick Reference

```bash
# Feature
feat(scope): add something new

# Bug fix
fix(scope): correct something

# Documentation
docs(scope): update something

# Refactoring
refactor(scope): restructure something

# Performance
perf(scope): optimize something

# Maintenance
chore(scope): update something
```
