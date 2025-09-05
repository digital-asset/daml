SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help
## help: Makefile: Prints this help message
help:
	@echo "Usage:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

.PHONY: up
## up: Docker compose: Start everything (blocking)
up:
	docker compose up --abort-on-container-exit

.PHONY: upd
## upd: Docker compose: Start everything (detached)
upd:
	docker compose up -d

.PHONY: down
## down: Docker compose: Stop everything and remove volumes
down:
	docker compose down --volumes

.PHONY: monitoring
## monitoring: Docker compose: Start only Prometheus, Loki & Grafana
monitoring:
	docker compose up grafana --abort-on-container-exit

.PHONY: canton
## daml: Docker compose: Start only Postgres and Canton
canton:
	docker compose up canton -d

.PHONY: prom
## prom: Prometheus: Access Web UI
prom:
	$$(command -v xdg-open || command -v open) http://localhost:9090/

.PHONY: prom-reload
## prom-reload: Prometheus: Reload configuration
prom-reload:
	docker exec -it daml_observability_prometheus-1 kill -HUP 1

.PHONY: prom-restart
## prom-restart: Prometheus: Restart
prom-restart:
	docker compose restart prometheus

.PHONY: prom-logs
## prom-logs: Prometheus: Follow logs (blocking)
prom-logs:
	docker logs -f daml_observability_prometheus

.PHONY: grafana
## grafana: Grafana: Access Web UI
grafana:
	$$(command -v xdg-open || command -v open) http://localhost:3000/

.PHONY: grafana-restart
## grafana-restart: Grafana: Restart
grafana-restart:
	docker compose restart grafana

.PHONY: grafana-logs
## grafana-logs: Grafana: Follow logs (blocking)
grafana-logs:
	docker logs -f daml_observability_grafana

.PHONY: console
## console: Canton: Open Console
console:
	docker exec -it daml_observability_canton_console bin/canton -c /canton/config/console.conf

.PHONY: debug
## debug: Canton: Open Shell
debug:
	docker exec -it daml_observability_canton_console /bin/bash

.PHONY: canton-restart
## canton-restart: Canton: Restart
canton-restart:
	docker compose restart canton

.PHONY: canton-logs
## canton-logs: Canton: Follow logs (blocking)
canton-logs:
	docker logs -f daml_observability_canton
