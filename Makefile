.PHONY: up ddl data

COMPOSE ?= docker compose

up:
	$(COMPOSE) up -d

ddl:
	bash ./scripts/apply_clickhouse_ddl.sh

data:
	bash ./scripts/load_kafka_data.sh
