.PHONY: help env up down logs status test lint validate trigger trigger-csv db-connect db-query clean rebuild ps restart

COMPOSE := docker compose

help:
	@echo "Available commands:"
	@echo "  make env          - Create .env from .env.example (if missing)"
	@echo "  make up           - Start services"
	@echo "  make down         - Stop services"
	@echo "  make logs         - Tail webserver logs"
	@echo "  make status       - Show container status"
	@echo "  make test         - Run the test suite (pytest)"
	@echo "  make lint         - Run ruff"
	@echo "  make validate     - Check that every DAG file imports cleanly"
	@echo "  make trigger      - Unpause + trigger elt_pipeline_dag"
	@echo "  make trigger-csv  - Unpause + trigger csv_to_db_dag"
	@echo "  make db-connect   - Open psql shell to database"
	@echo "  make db-query     - Show tables and row counts"
	@echo "  make clean        - Remove logs and output files"
	@echo "  make rebuild      - Clean start (down -v, rebuild image, up)"
	@echo "  make restart      - Restart all services"

env:
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example - edit the secrets before starting.")
	@test -f .env && echo ".env is present."

up: env
	@echo "Starting services..."
	$(COMPOSE) up -d --build
	@echo "Services started. Check status with: make status"

down:
	@echo "Stopping services..."
	$(COMPOSE) down
	@echo "Services stopped."

logs:
	@echo "Tailing webserver logs (Ctrl+C to exit)..."
	$(COMPOSE) logs -f webserver

status ps:
	$(COMPOSE) ps

test:
	pytest -q

lint:
	ruff check .

validate:
	@echo "Checking DAG imports..."
	$(COMPOSE) exec scheduler airflow dags list-import-errors
	$(COMPOSE) exec scheduler airflow dags list

trigger:
	$(COMPOSE) exec scheduler airflow dags unpause elt_pipeline_dag
	$(COMPOSE) exec scheduler airflow dags trigger elt_pipeline_dag
	@echo "Triggered. Monitor in the UI or with: make logs"

trigger-csv:
	$(COMPOSE) exec scheduler airflow dags unpause csv_to_db_dag
	$(COMPOSE) exec scheduler airflow dags trigger csv_to_db_dag
	@echo "Triggered. Monitor in the UI or with: make logs"

db-connect:
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-airflow_user} -d $${POSTGRES_DB:-airflow_db}

db-query:
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-airflow_user} -d $${POSTGRES_DB:-airflow_db} -c "\dt"
	@echo ""
	@echo "Row counts:"
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-airflow_user} -d $${POSTGRES_DB:-airflow_db} -c "\
	  SELECT 'raw_sales_data' AS table_name, COUNT(*) AS row_count FROM raw_sales_data \
	  UNION ALL SELECT 'transformed_sales', COUNT(*) FROM transformed_sales \
	  UNION ALL SELECT 'region_sales_summary', COUNT(*) FROM region_sales_summary;"

clean:
	@echo "Cleaning logs and output files..."
	rm -rf logs/*
	rm -rf output/*.xlsx
	@echo "Clean complete."

rebuild:
	@echo "Performing clean rebuild (this deletes the database volume)..."
	$(COMPOSE) down -v
	rm -rf logs/* output/*.xlsx
	$(COMPOSE) build --no-cache
	$(COMPOSE) up -d
	@echo "Rebuild complete. Monitor startup with: make status"

restart:
	$(COMPOSE) restart
	@echo "Services restarted."
