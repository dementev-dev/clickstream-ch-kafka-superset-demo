.PHONY: up down clean ddl data transform logs \
        reload-monitoring recover-monitoring \
        superset-init superset-dashboard superset-ui superset-restart

COMPOSE ?= docker compose

# ============================================================================
# Основные команды
# ============================================================================

up:
	$(COMPOSE) up -d

# Остановить и удалить контейнеры/сети текущего проекта
down:
	$(COMPOSE) down

# Полная очистка окружения проекта (включая volumes)
clean:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f --tail=200 $(service)

# ============================================================================
# ETL Pipeline
# ============================================================================

ddl:
	bash ./scripts/apply_clickhouse_ddl.sh

data:
	bash ./scripts/load_kafka_data.sh

transform:
	bash ./scripts/run_batch.sh

# Перезагрузка конфигурации мониторинга (после изменений в provisioning)
reload-monitoring:
	@echo "=== Перезагрузка сервисов мониторинга ==="
	$(COMPOSE) up -d prometheus grafana kafka-exporter statsd-exporter
	$(COMPOSE) restart prometheus statsd-exporter
	@echo "=== Перезагрузка provisioning Grafana ==="
	@sleep 2
	@curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload && echo " [datasources]"
	@curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload && echo " [dashboards]"
	@curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload && echo " [alerting]"
	@echo "=== Проверка ==="
	@curl -s http://localhost:9090/api/v1/targets | grep -o '"job":"[^"]*"' | sort | uniq

# Восстановление мониторинга после сбоев (например, out of bounds / пустые дашборды)
recover-monitoring:
	@echo "=== Восстановление мониторинга (жесткий режим) ==="
	$(COMPOSE) rm -sf prometheus statsd-exporter
	$(COMPOSE) up -d prometheus grafana kafka-exporter statsd-exporter
	$(COMPOSE) restart airflow-scheduler airflow-webserver
	@echo "=== Перезагрузка provisioning Grafana ==="
	@sleep 2
	@curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/datasources/reload && echo " [datasources]"
	@curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/dashboards/reload && echo " [dashboards]"
	@curl -s -u admin:admin -X POST http://localhost:3000/api/admin/provisioning/alerting/reload && echo " [alerting]"
	@echo "=== Проверка targets ==="
	@curl -s http://localhost:9090/api/v1/targets | grep -o '"job":"[^"]*"' | sort | uniq
	@curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"[^"]*"' | sort | uniq -c

# ============================================================================
# Superset команды
# ============================================================================

# Инициализация Superset (подключение к ClickHouse + датасеты)
superset-init:
	$(COMPOSE) up -d postgres-metadata clickhouse
	$(COMPOSE) up --abort-on-container-exit --exit-code-from superset-init superset-init
	$(COMPOSE) up -d --no-deps superset

# Создание дашборда с чартами
superset-dashboard:
	$(COMPOSE) exec -T superset bash -c "python /app/superset_init/create_dashboard.py"

# Открыть Superset UI
superset-ui:
	@echo "Superset доступен по адресу: http://localhost:8088"
	@echo "Логин: admin / Пароль: admin"

# Перезапуск Superset
superset-restart:
	$(COMPOSE) restart superset
