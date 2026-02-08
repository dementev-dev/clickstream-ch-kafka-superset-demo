.PHONY: up ddl data transform reload-monitoring

COMPOSE ?= docker compose

up:
	$(COMPOSE) up -d

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
