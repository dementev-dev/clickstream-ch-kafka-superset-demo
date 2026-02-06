.PHONY: up ddl data transform

COMPOSE ?= docker compose

up:
	$(COMPOSE) up -d

ddl:
	bash ./scripts/apply_clickhouse_ddl.sh

data:
	bash ./scripts/load_kafka_data.sh

transform:
	bash ./scripts/run_batch.sh
