.DEFAULT_GOAL := run-all
#!make
include .env
export $(shell sed 's/=.*//' .env)

.PHONY: dump
dump:
	/opt/homebrew/opt/libpq/bin/pg_restore -U ${POSTGRES_USER} -d ${POSTGRES_DB} -h localhost -p 5432 -v ./dump/_adventureworkslt.dump

.PHONY: migrate
migrate:
	make migrate-status
	goose -dir ./migrations postgres "${URI}" up

.PHONY: remove-migration
remove-migration:
	make migrate-status
	goose -dir ./migrations postgres "${URI}" down

.PHONY: down-to-migration
down-to-migration:
	make migrate-status
	goose -dir ./migrations postgres "${URI}" down-to $(m)

.PHONY: migrate-status
migrate-status:
	@echo ${}
	goose -dir ./migrations postgres "${URI}" status

.PHONY: run
run:
	docker compose up --build -d

.PHONE: down
down:
	docker compose down --remove-orphans

.PHONY: down-all
down-all: down remove-volumes

.PHONE: remove-volumes
remove-volumes:
	docker volume rm --force sql_final_task_pg_data

.PHONE: apply-python-scripts
apply-python-scripts:
	python3 start_planing.py
	python3 change_plan.py
	python3 accept_plan.py

run-all: run
	sleep 3
	-make dump
	make migrate
