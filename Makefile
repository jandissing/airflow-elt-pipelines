.PHONY: help setup up down logs status validate trigger db-connect db-query clean rebuild ps restart

help:
	@echo "Available commands:"
	@echo "  make setup        - Create directories and start services"
	@echo "  make up           - Start services"
	@echo "  make down         - Stop services"
	@echo "  make logs         - Tail webserver logs"
	@echo "  make status       - Show docker-compose ps"
	@echo "  make validate     - Validate DAG syntax"
	@echo "  make trigger      - Manually trigger elt_pipeline_dag"
	@echo "  make db-connect   - Open psql shell to database"
	@echo "  make db-query     - Show all tables in database"
	@echo "  make clean        - Remove logs and output files"
	@echo "  make rebuild      - Clean start (down -v, remove caches, up)"
	@echo "  make ps           - Show container status"
	@echo "  make restart      - Restart all services"

setup:
	@echo "Creating project directories..."
	mkdir -p dags logs plugins output
	@echo "Starting services..."
	docker-compose up -d
	@echo "Setup complete! Waiting for services to start..."
	@echo "Monitor startup with: make status"

up:
	@echo "Starting services..."
	docker-compose up -d
	@echo "Services started. Check status with: make status"

down:
	@echo "Stopping services..."
	docker-compose down
	@echo "Services stopped."

logs:
	@echo "Tailing webserver logs (Ctrl+C to exit)..."
	docker-compose logs -f webserver

status:
	@echo "Docker Compose Status:"
	docker-compose ps

validate:
	@echo "Validating DAG syntax..."
	docker-compose exec webserver python -m py_compile dags/elt_pipeline_dag.py
	@echo "DAG syntax is valid!"

trigger:
	@echo "Triggering elt_pipeline_dag..."
	docker-compose exec webserver airflow dags trigger elt_pipeline_dag
	@echo "DAG triggered! Monitor in UI or with: make logs"

db-connect:
	@echo "Connecting to PostgreSQL database..."
	docker-compose exec postgres psql -U airflow_user -d airflow_db

db-query:
	@echo "Showing all tables in database..."
	docker-compose exec postgres psql -U airflow_user -d airflow_db -c "\dt"
	@echo ""
	@echo "Row counts:"
	docker-compose exec postgres psql -U airflow_user -d airflow_db -c "SELECT 'raw_sales_data' as table_name, COUNT(*) as row_count FROM raw_sales_data UNION ALL SELECT 'transformed_sales', COUNT(*) FROM transformed_sales;"

clean:
	@echo "Cleaning logs and output files..."
	rm -rf logs/*
	rm -rf output/*.xlsx
	@echo "Clean complete."

rebuild:
	@echo "Performing clean rebuild..."
	@echo "1. Removing containers and volumes..."
	docker-compose down -v
	@echo "2. Pruning Docker system..."
	docker system prune -f
	@echo "3. Removing local caches..."
	rm -rf logs/*
	rm -rf output/*.xlsx
	@echo "4. Starting fresh..."
	docker-compose up -d
	@echo "Rebuild complete! Services are starting..."
	@echo "Monitor startup with: make status"

ps:
	@echo "Container Status:"
	docker-compose ps

restart:
	@echo "Restarting all services..."
	docker-compose restart
	@echo "Services restarted. Check status with: make status"
