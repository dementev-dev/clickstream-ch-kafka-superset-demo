#!/usr/bin/env bash
#
# Повторяемая проверка, что DM-витрины и Superset metadata работают на данных
# стартовой истории генератора, а не на архивном сиде 2022 года.

set -euo pipefail

COMPOSE_BIN="${COMPOSE_BIN:-docker compose}"
CLICKHOUSE_SERVICE="${CLICKHOUSE_SERVICE:-clickhouse}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-123456}"
GEN_MODEL_T0="${GEN_MODEL_T0:-2026-01-01T00:00:00+00:00}"
GEN_MODEL_T_END="${GEN_MODEL_T_END:-2026-01-01T06:00:00+00:00}"
REQUIRE_SUPERSET="${REQUIRE_SUPERSET:-1}"

fail() {
  echo "Ошибка: $*" >&2
  exit 1
}

clickhouse_datetime_literal() {
  local value="$1"
  value="${value/T/ }"
  value="${value%Z}"
  if [[ "${value}" =~ ^(.*)[+-][0-9]{2}:[0-9]{2}$ ]]; then
    value="${BASH_REMATCH[1]}"
  fi
  echo "${value}"
}

ch_query() {
  ${COMPOSE_BIN} exec -T "${CLICKHOUSE_SERVICE}" clickhouse-client \
    --user="${CLICKHOUSE_USER}" \
    --password="${CLICKHOUSE_PASSWORD}" \
    --query "$1"
}

pg_query() {
  ${COMPOSE_BIN} exec -T postgres-metadata psql \
    -U airflow \
    -d superset \
    -At \
    -c "$1" | tr -d '\r'
}

CH_MODEL_T0="$(clickhouse_datetime_literal "${GEN_MODEL_T0}")"
CH_MODEL_T_END="$(clickhouse_datetime_literal "${GEN_MODEL_T_END}")"

echo "=== Проверка ClickHouse: данные генерации в DM ==="

stats_query="
WITH
    toDateTime64('${CH_MODEL_T0}', 6) AS t0,
    toDateTime64('${CH_MODEL_T_END}', 6) AS t_end
SELECT
    count() AS events,
    uniqExact(click_id) AS visits,
    uniqExact(user_domain_id) AS users,
    toString(min(event_ts)) AS min_event_ts,
    toString(max(event_ts)) AS max_event_ts,
    users < visits AND visits < events AS pyramid_ok,
    min(event_ts) >= t0 AND max(event_ts) < t_end AS half_open_ok,
    countIf(toYear(event_ts) = 2022) = 0 AS no_2022_rows,
    hex(sipHash128(groupArray(tuple(event_id, click_id, user_domain_id, event_ts, page_url_path)))) AS digest
FROM
(
    SELECT
        event_id,
        click_id,
        user_domain_id,
        event_ts,
        page_url_path
    FROM dm.v_events_enriched
    WHERE event_ts >= t0 AND event_ts < t_end
    ORDER BY event_id
)
FORMAT TabSeparated"

stats="$(ch_query "${stats_query}")"
IFS=$'\t' read -r events visits users min_event_ts max_event_ts pyramid_ok half_open_ok no_2022_rows digest <<< "${stats}"

[[ "${events}" =~ ^[0-9]+$ ]] || fail "не удалось прочитать число событий из dm.v_events_enriched"
(( events > 0 )) || fail "dm.v_events_enriched пустая в модельном диапазоне"
[[ "${pyramid_ok}" == "1" ]] || fail "нарушена пирамида users < visits < events"
[[ "${half_open_ok}" == "1" ]] || fail "данные вышли за диапазон [GEN_MODEL_T0, GEN_MODEL_T_END)"
[[ "${no_2022_rows}" == "1" ]] || fail "найдены строки 2022 года, похожие на архивный сид"

echo "events=${events}"
echo "visits=${visits}"
echo "users=${users}"
echo "min_event_ts=${min_event_ts}"
echo "max_event_ts=${max_event_ts}"
echo "digest=${digest}"

echo ""
echo "=== Проверка ClickHouse: возвраты пользователей ==="

returns_query="
WITH
    toDateTime64('${CH_MODEL_T0}', 6) AS t0,
    toDateTime64('${CH_MODEL_T_END}', 6) AS t_end,
    users AS (
        SELECT user_domain_id, uniqExact(click_id) AS visits
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
          AND user_domain_id IS NOT NULL
        GROUP BY user_domain_id
    )
SELECT
    count() AS users,
    countIf(visits > 1) AS returning_users,
    returning_users / users AS returning_share,
    max(visits) AS max_visits_per_user
FROM users
FORMAT TabSeparated"

returns="$(ch_query "${returns_query}")"
IFS=$'\t' read -r total_users returning_users returning_share max_visits_per_user <<< "${returns}"

[[ "${total_users}" =~ ^[0-9]+$ ]] || fail "не удалось прочитать число пользователей"
(( total_users > 0 )) || fail "нет пользователей для проверки возвратов"
(( returning_users > 0 )) || fail "нет пользователей с повторными визитами"

echo "users=${total_users}"
echo "returning_users=${returning_users}"
echo "returning_share=${returning_share}"
echo "max_visits_per_user=${max_visits_per_user}"

echo ""
echo "=== Проверка ClickHouse: форма длины визита ==="

visit_shape_query="
WITH
    toDateTime64('${CH_MODEL_T0}', 6) AS t0,
    toDateTime64('${CH_MODEL_T_END}', 6) AS t_end,
    30 AS max_session_events,
    sessions AS (
        SELECT
            click_id,
            count() AS events_count,
            dateDiff('second', min(event_ts), max(event_ts)) AS duration_sec
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
        GROUP BY click_id
    )
SELECT
    count() AS visits,
    countIf(events_count <= 2) / visits AS short_visit_share,
    quantileExact(0.5)(events_count) AS median_events_per_visit,
    avg(events_count) AS avg_events_per_visit,
    countIf(events_count = max_session_events) / visits AS capped_visit_share,
    quantileExact(0.5)(duration_sec) AS median_duration_sec,
    quantileExact(0.95)(duration_sec) AS p95_duration_sec,
    max(events_count) AS max_events_per_visit
FROM sessions
FORMAT TabSeparated"

visit_shape="$(ch_query "${visit_shape_query}")"
IFS=$'\t' read -r shape_visits short_visit_share median_events_per_visit avg_events_per_visit capped_visit_share median_duration_sec p95_duration_sec max_events_per_visit <<< "${visit_shape}"

[[ "${shape_visits}" =~ ^[0-9]+$ ]] || fail "не удалось прочитать число визитов"
(( shape_visits > 0 )) || fail "нет визитов для проверки формы"
(( max_events_per_visit <= 30 )) || fail "длина визита превысила GEN_MAX_SESSION_EVENTS"

echo "visits=${shape_visits}"
echo "short_visit_share=${short_visit_share}"
echo "median_events_per_visit=${median_events_per_visit}"
echo "avg_events_per_visit=${avg_events_per_visit}"
echo "capped_visit_share=${capped_visit_share}"
echo "median_duration_sec=${median_duration_sec}"
echo "p95_duration_sec=${p95_duration_sec}"
echo "max_events_per_visit=${max_events_per_visit}"

echo ""
echo "=== Проверка ClickHouse: ordered funnel ==="

ordered_funnel_query="
WITH
    toDateTime64('${CH_MODEL_T0}', 6) AS t0,
    toDateTime64('${CH_MODEL_T_END}', 6) AS t_end,
    sessions AS (
        SELECT
            click_id,
            minIf(event_ts, page_url_path = '/home') AS home_ts,
            minIf(event_ts, page_url_path IN ('/product_a', '/product_b')) AS product_ts,
            minIf(event_ts, page_url_path = '/cart') AS cart_ts,
            minIf(event_ts, page_url_path = '/payment') AS payment_ts,
            minIf(event_ts, page_url_path = '/confirmation') AS confirmation_ts
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
        GROUP BY click_id
    )
SELECT
    countIf(home_ts IS NOT NULL) AS home,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts) AS products,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts AND cart_ts > product_ts) AS cart,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts AND cart_ts > product_ts AND payment_ts > cart_ts) AS payment,
    countIf(home_ts IS NOT NULL AND product_ts > home_ts AND cart_ts > product_ts AND payment_ts > cart_ts AND confirmation_ts > payment_ts) AS confirmation,
    products <= home AND cart <= products AND payment <= cart AND confirmation <= payment AS monotonic_ok,
    confirmation / home AS confirmation_share
FROM sessions
FORMAT TabSeparated"

ordered_funnel="$(ch_query "${ordered_funnel_query}")"
IFS=$'\t' read -r ordered_home ordered_products ordered_cart ordered_payment ordered_confirmation ordered_monotonic_ok ordered_confirmation_share <<< "${ordered_funnel}"

[[ "${ordered_home}" =~ ^[0-9]+$ ]] || fail "не удалось прочитать ordered funnel"
(( ordered_home > 0 )) || fail "ordered funnel: нет /home"
[[ "${ordered_monotonic_ok}" == "1" ]] || fail "ordered funnel не монотонен"

echo "home=${ordered_home}"
echo "products=${ordered_products}"
echo "cart=${ordered_cart}"
echo "payment=${ordered_payment}"
echo "confirmation=${ordered_confirmation}"
echo "monotonic_ok=${ordered_monotonic_ok}"
echo "confirmation_share=${ordered_confirmation_share}"

echo ""
echo "=== Проверка ClickHouse: contains funnel ==="

contains_funnel_query="
WITH
    toDateTime64('${CH_MODEL_T0}', 6) AS t0,
    toDateTime64('${CH_MODEL_T_END}', 6) AS t_end,
    sessions AS (
        SELECT
            click_id,
            countIf(page_url_path = '/home') > 0 AS has_home,
            countIf(page_url_path IN ('/product_a', '/product_b')) > 0 AS has_product,
            countIf(page_url_path = '/cart') > 0 AS has_cart,
            countIf(page_url_path = '/payment') > 0 AS has_payment,
            countIf(page_url_path = '/confirmation') > 0 AS has_confirmation
        FROM dm.v_events_enriched
        WHERE event_ts >= t0 AND event_ts < t_end
        GROUP BY click_id
    )
SELECT
    countIf(has_home) AS home,
    countIf(has_home AND has_product) AS products,
    countIf(has_home AND has_product AND has_cart) AS cart,
    countIf(has_home AND has_product AND has_cart AND has_payment) AS payment,
    countIf(has_home AND has_product AND has_cart AND has_payment AND has_confirmation) AS confirmation,
    products <= home AND cart <= products AND payment <= cart AND confirmation <= payment AS monotonic_ok,
    confirmation / home AS confirmation_share
FROM sessions
FORMAT TabSeparated"

contains_funnel="$(ch_query "${contains_funnel_query}")"
IFS=$'\t' read -r contains_home contains_products contains_cart contains_payment contains_confirmation contains_monotonic_ok contains_confirmation_share <<< "${contains_funnel}"

[[ "${contains_home}" =~ ^[0-9]+$ ]] || fail "не удалось прочитать contains funnel"
(( contains_home > 0 )) || fail "contains funnel: нет /home"
[[ "${contains_monotonic_ok}" == "1" ]] || fail "contains funnel не монотонен"

echo "home=${contains_home}"
echo "products=${contains_products}"
echo "cart=${contains_cart}"
echo "payment=${contains_payment}"
echo "confirmation=${contains_confirmation}"
echo "monotonic_ok=${contains_monotonic_ok}"
echo "confirmation_share=${contains_confirmation_share}"

echo ""
echo "=== Проверка ClickHouse: основные DM-витрины не пустые ==="

views_query="
SELECT source, rows
FROM
(
    SELECT 'dm.v_events_enriched' AS source, count() AS rows FROM dm.v_events_enriched
    UNION ALL SELECT 'dm.v_daily_traffic', count() FROM dm.v_daily_traffic
    UNION ALL SELECT 'dm.v_top_pages_daily', count() FROM dm.v_top_pages_daily
    UNION ALL SELECT 'dm.v_utm_effectiveness', count() FROM dm.v_utm_effectiveness
    UNION ALL SELECT 'dm.v_session_overview', count() FROM dm.v_session_overview
    UNION ALL SELECT 'dm.dq_summary', count() FROM dm.dq_summary
)
ORDER BY source
FORMAT TabSeparated"

while IFS=$'\t' read -r source rows; do
  [[ -n "${source}" ]] || continue
  [[ "${rows}" =~ ^[0-9]+$ ]] || fail "не удалось прочитать число строк для ${source}"
  (( rows > 0 )) || fail "${source} пустая"
  echo "${source}=${rows}"
done < <(ch_query "${views_query}")

if [[ "${REQUIRE_SUPERSET}" != "1" ]]; then
  echo ""
  echo "Проверка Superset пропущена: REQUIRE_SUPERSET=${REQUIRE_SUPERSET}"
  exit 0
fi

echo ""
echo "=== Проверка Superset: datasets, charts и dashboard созданы ==="

if ! ${COMPOSE_BIN} ps --services --filter "status=running" | grep -qx "superset"; then
  fail "сервис superset не запущен"
fi

${COMPOSE_BIN} exec -T superset curl -fsS http://localhost:8088/health >/dev/null \
  || fail "Superset health endpoint не отвечает"

dataset_count="$(pg_query "
SELECT count(*)
FROM tables
WHERE schema = 'dm'
  AND table_name IN (
    'v_events_enriched',
    'v_daily_traffic',
    'v_utm_effectiveness',
    'v_top_pages_daily',
    'v_session_overview',
    'dq_summary'
  );")"

dashboard_count="$(pg_query "
SELECT count(*)
FROM dashboards
WHERE slug = 'ecommerce-analytics';")"

chart_count="$(pg_query "
SELECT count(*)
FROM dashboard_slices ds
JOIN dashboards d ON d.id = ds.dashboard_id
WHERE d.slug = 'ecommerce-analytics';")"

[[ "${dataset_count}" == "6" ]] || fail "ожидалось 6 Superset datasets, найдено ${dataset_count}"
[[ "${dashboard_count}" == "1" ]] || fail "dashboard ecommerce-analytics не найден"
(( chart_count > 0 )) || fail "dashboard ecommerce-analytics не связан с chart"

echo "superset_datasets=${dataset_count}"
echo "superset_dashboards=${dashboard_count}"
echo "superset_dashboard_charts=${chart_count}"
echo "dashboard_url=http://localhost:8088/superset/dashboard/ecommerce-analytics/"

echo ""
echo "=== Проверка Superset: dashboard открывается и читает ClickHouse ==="

superset_probe="$(
  ${COMPOSE_BIN} exec -T superset bash -s <<'PY'
set -euo pipefail
python - <<'PYTHON'
import json
import urllib.request

from sqlalchemy import text

from superset.app import create_app

app = create_app()
with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard

    dashboard = (
        db.session.query(Dashboard)
        .filter_by(slug="ecommerce-analytics")
        .one()
    )

    database = (
        db.session.query(Database)
        .filter_by(database_name="clickhouse_dwh")
        .one()
    )
    with database.get_sqla_engine() as engine:
      with engine.connect() as connection:
        events = connection.execute(
            text("SELECT count() FROM dm.v_events_enriched")
        ).scalar()

login_payload = json.dumps(
    {
        "username": "admin",
        "password": "admin",
        "provider": "db",
        "refresh": True,
    }
).encode("utf-8")

login_request = urllib.request.Request(
    "http://localhost:8088/api/v1/security/login",
    data=login_payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(login_request, timeout=10) as response:
    login_status = response.status
    token = json.loads(response.read().decode("utf-8"))["access_token"]

dashboard_request = urllib.request.Request(
    f"http://localhost:8088/api/v1/dashboard/{dashboard.id}",
    headers={"Authorization": f"Bearer {token}"},
    method="GET",
)
with urllib.request.urlopen(dashboard_request, timeout=10) as response:
    dashboard_status = response.status
    dashboard_payload = json.loads(response.read().decode("utf-8"))

dashboard_title = dashboard_payload["result"]["dashboard_title"]

print(f"login_api_status={login_status}")
print(f"dashboard_api_status={dashboard_status}")
print(f"dashboard_title={dashboard_title}")
print(f"superset_clickhouse_events={events}")

if login_status >= 400:
    raise SystemExit("Superset login API failed")
if dashboard_status >= 400:
    raise SystemExit("Superset dashboard API did not open")
if "E-commerce Analytics" not in dashboard_title:
    raise SystemExit("Unexpected dashboard title")
if not isinstance(events, int) or events <= 0:
    raise SystemExit("Superset ClickHouse query returned no data")
PYTHON
PY
)"

echo "${superset_probe}"
