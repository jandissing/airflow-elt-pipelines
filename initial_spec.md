# Local Airflow + PostgreSQL ELT Pipeline - Technical Specifications

## 1. PROJECT OVERVIEW

Build a containerized ELT (Extract, Load, Transform) pipeline using:
- Apache Airflow 2.9.1 as orchestration engine
- PostgreSQL 15 as data source and Airflow metadata backend
- Python 3.11 runtime
- Docker Compose for local development
- Pandas for data transformation
- OpenPyXL for Excel export

**Expected Outcome**: A fully functional local environment where a DAG extracts data from PostgreSQL, transforms it, loads transformed results back, and exports to timestamped Excel files.

---

## 2. ARCHITECTURE COMPONENTS

### 2.1 Services (Docker Compose)

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| postgres | postgres:15-alpine | 5432 | Data storage & metadata DB |
| webserver | apache/airflow:2.9.1-python3.11 | 8080 | Airflow UI and REST API |
| scheduler | apache/airflow:2.9.1-python3.11 | (internal) | DAG scheduler and executor |

### 2.2 Environment Configuration

**Airflow Settings:**
- Executor: LocalExecutor (not DistributedExecutor)
- Dags Folder: `/opt/airflow/dags`
- Load Examples: False
- Fernet Key: `d6Vefz3G9U_wnXnTE8jV7tIcaOP6p8qcTu_QWLEHQkU=`
- Webserver Secret Key: `secret_key_here`

**Database Connection:**
- Type: PostgreSQL
- Host: `postgres` (Docker internal DNS)
- Port: 5432
- User: `airflow`
- Password: `airflow`
- Database: `airflow`
- Connection String: `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`

**Additional Python Packages:**
- postgres
- psycopg2-binary
- openpyxl
- pandas
- sqlalchemy

---

## 3. FILE STRUCTURE

```
project-root/
├── docker-compose.yml          # Orchestration definition
├── init-db.sql                 # PostgreSQL initialization script
├── dags/
│   └── elt_pipeline_dag.py     # Main DAG definition
├── logs/                       # (auto-created) Airflow logs
├── plugins/                    # (auto-created) Custom Airflow plugins
├── output/                     # (auto-created) Generated Excel exports
├── README.md                   # Setup guide
├── Makefile                    # Common commands
└── .gitignore                  # Git ignore patterns
```

---

## 4. FILE SPECIFICATIONS

### 4.1 docker-compose.yml

**Purpose**: Define all services and their configurations

**Key Requirements:**
- Version: 3.8 or higher
- Three services: postgres, webserver, scheduler
- Postgres service:
  - Uses `postgres:15-alpine`
  - Environment: POSTGRES_USER=airflow, POSTGRES_PASSWORD=airflow, POSTGRES_DB=airflow
  - Mounts: `/docker-entrypoint-initdb.d/init-db.sql` for initialization
  - Volume: `postgres_data` for persistence
  - Healthcheck: pg_isready command
  - Port mapping: 5432:5432

- Webserver service:
  - Uses `apache/airflow:2.9.1-python3.11`
  - Depends on: postgres (service_healthy condition)
  - Environment variables:
    - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
    - AIRFLOW__CORE__LOAD_EXAMPLES=False
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__CORE__FERNET_KEY=d6Vefz3G9U_wnXnTE8jV7tIcaOP6p8qcTu_QWLEHQkU=
    - AIRFLOW__WEBSERVER__SECRET_KEY=secret_key_here
    - _PIP_ADDITIONAL_REQUIREMENTS=postgres psycopg2-binary openpyxl pandas sqlalchemy
  - Volumes: dags, logs, plugins, output folders
  - Port: 8080:8080
  - Command: Run `airflow db upgrade`, create admin user (idempotent), start webserver
  - Healthcheck: curl to http://localhost:8080/health

- Scheduler service:
  - Uses `apache/airflow:2.9.1-python3.11`
  - Same environment as webserver
  - Depends on: webserver (service_healthy)
  - Volumes: same as webserver
  - Command: `airflow scheduler`

- Named volume: `postgres_data`
- Network: named network `airflow-network`

---

### 4.2 init-db.sql

**Purpose**: Initialize PostgreSQL database with schema and sample data

**Schema Creation:**

1. Table: `raw_sales_data`
   - Columns:
     - id (SERIAL PRIMARY KEY)
     - customer_name (VARCHAR 255)
     - product_name (VARCHAR 255)
     - quantity (INTEGER)
     - unit_price (DECIMAL 10,2)
     - sale_date (DATE)
     - region (VARCHAR 50)

2. Table: `transformed_sales`
   - Columns:
     - id (SERIAL PRIMARY KEY)
     - customer_name (VARCHAR 255)
     - product_name (VARCHAR 255)
     - total_amount (DECIMAL 10,2)
     - sale_date (DATE)
     - region (VARCHAR 50)
     - transformed_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

**Sample Data** (Insert into raw_sales_data):
- 8 rows of sample Brazilian sales data
- Mix of products: Laptop, Mouse, Keyboard, Monitor, Headphones, Webcam
- Quantities: 1-5 units each
- Unit prices: 150-3500
- Dates: Spread across Jan 15-22, 2024
- Regions: Southeast, South, Northeast, North

**Permissions:**
- Grant all privileges on public schema tables to airflow user
- Grant usage on all sequences to airflow user

---

### 4.3 dags/elt_pipeline_dag.py

**Purpose**: Define the Airflow DAG with four-task ELT pipeline

**DAG Metadata:**
- dag_id: `elt_pipeline_dag`
- owner: `airflow`
- retries: 1
- retry_delay: 5 minutes
- start_date: datetime(2024, 1, 1)
- schedule_interval: `0 9 * * *` (daily at 9 AM)
- catchup: False
- tags: ['elt', 'data-engineering']

**Constants:**
- DB_CONNECTION = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'
- OUTPUT_DIR = '/opt/airflow/output'

**Task 1: extract (PythonOperator)**
- Function: `extract_raw_data(**context)`
- Behavior:
  - Create SQLAlchemy engine to PostgreSQL
  - Execute: `SELECT * FROM raw_sales_data ORDER BY sale_date`
  - Read results into Pandas DataFrame
  - Log: number of rows extracted
  - Push raw_data (as JSON string) to xcom with key 'raw_data'
  - Return: row count

**Task 2: transform (PythonOperator)**
- Function: `transform_data(**context)`
- Behavior:
  - Pull raw_data from xcom (from extract task)
  - Create new column: total_amount = quantity × unit_price
  - Select columns: customer_name, product_name, total_amount, sale_date, region
  - Log transformation details
  - Push transformed_data (as JSON) to xcom with key 'transformed_data'
  - Return: row count

**Task 3: load (PythonOperator)**
- Function: `load_to_database(**context)`
- Behavior:
  - Pull transformed_data from xcom (from transform task)
  - Create SQLAlchemy engine
  - Delete all rows from transformed_sales table (cleanup)
  - Use df.to_sql() with if_exists='append' to load data
  - Log: number of rows loaded
  - Close engine

**Task 4: export (PythonOperator)**
- Function: `export_to_excel(**context)`
- Behavior:
  - Pull transformed_data from xcom
  - Create output directory if doesn't exist
  - Generate filename: `sales_report_{YYYYMMDD_HHMMSS}.xlsx`
  - Use pd.ExcelWriter with openpyxl engine
  - Write data to sheet named 'Sales Data'
  - Auto-adjust column widths (max 50 chars)
  - Log summary statistics:
    - Total records
    - Total revenue (sum of total_amount)
    - Revenue by region (groupby and sum)

**Task Dependencies:**
- extract >> transform >> [load, export]
- Both load and export depend on transform
- load and export run in parallel

---

### 4.4 README.md

**Sections to Include:**

1. Prerequisites
   - Docker & Docker Compose required
   - 4GB RAM minimum
   - Ports 5432, 8080 must be available

2. Quick Start (5 sections)
   - Directory structure creation
   - File placement
   - docker-compose up command
   - Wait for startup
   - Access http://localhost:8080

3. Running the DAG
   - How to trigger from UI
   - Where to find output files

4. Pipeline Breakdown
   - What each stage does
   - Data flow explanation

5. Database Details
   - Connection string
   - Port, user, password
   - Table schemas
   - How to connect from host

6. Useful Commands
   - View logs
   - Stop services
   - Trigger DAG manually
   - Validate DAG syntax
   - Fresh restart

7. Customization
   - Adding more sample data
   - Modifying DAG logic
   - Changing schedule
   - Auto-reload behavior

8. Output Files
   - Where Excel files are saved
   - Naming convention
   - No overwrites

9. Troubleshooting
   - Connection refused
   - DAG doesn't appear
   - Memory issues

10. Next Steps
    - Incremental loads
    - Data quality checks
    - Alerts and monitoring

---

### 4.5 Makefile

**Targets to Implement:**

- `help` - Display all available commands
- `setup` - Create directories, copy DAG, start services
- `up` - Start services
- `down` - Stop services
- `logs` - Tail webserver logs
- `status` - Show docker-compose ps
- `validate` - Validate DAG syntax
- `trigger` - Manually trigger elt_pipeline_dag
- `db-connect` - Open psql shell to database
- `db-query` - Show all tables in database
- `clean` - Remove logs and output files
- `rebuild` - Clean start (down -v, remove caches, up)
- `ps` - Show container status
- `restart` - Restart all services

**Output**: Each target should echo confirmation messages

---

### 4.6 .gitignore

**Patterns to Ignore:**

- Airflow: airflow.db, airflow-webserver.pid, logs/, dags/__pycache__/
- Generated: output/*.xlsx, output/*.csv
- Database: postgres_data/
- Python: __pycache__/, *.py[cod], *.egg-info/, venv/, env/
- IDE: .vscode/, .idea/, *.swp, *.swo
- OS: .DS_Store, Thumbs.db
- Docker: .dockerignore

---

## 5. SETUP WORKFLOW

1. Create project directory
2. Create subdirectories: dags/, logs/, plugins/, output/
3. Place all 6 files in root directory
4. Copy elt_pipeline_dag.py into dags/ folder
5. Run: `docker-compose up -d`
6. Wait 30-60 seconds for services to start
7. Access: http://localhost:8080
8. Login: admin/admin
9. Trigger DAG from UI or CLI

---

## 6. EXPECTED BEHAVIOR

### Initial Startup
1. PostgreSQL starts first and runs init-db.sql
2. Webserver waits for Postgres health check
3. Webserver runs migrations and creates admin user
4. Scheduler starts and picks up DAG definition
5. ~60 seconds total to full readiness

### DAG Execution
1. extract task: reads 8 rows from raw_sales_data
2. transform task: creates total_amount column
3. Parallel execution:
   - load task: inserts 8 rows into transformed_sales
   - export task: creates timestamped Excel file
4. All tasks log details to Airflow UI and container logs

### Output File
- Location: output/ directory (mounted from host)
- Naming: sales_report_YYYYMMDD_HHMMSS.xlsx
- Format: Single sheet "Sales Data" with headers
- Formatting: Auto-sized columns
- Console log includes revenue summary by region

---

## 7. SUCCESS CRITERIA

✅ All three containers (postgres, webserver, scheduler) running  
✅ Airflow UI accessible at http://localhost:8080  
✅ DAG visible in UI under "elt_pipeline_dag"  
✅ Manual trigger executes all 4 tasks successfully  
✅ Excel file generated in output/ with correct data  
✅ Database tables show transformed data in transformed_sales  
✅ No errors in scheduler or webserver logs  
✅ Schedule triggers automatically at 9 AM daily  

---

## 8. OPTIONAL ENHANCEMENTS

- Add error notifications
- Implement incremental loads (track last run timestamp)
- Add data quality checks via SQL assertions
- Extend with dbt for complex transformations
- Add monitoring/alerting via email or Slack
- Implement archival of old Excel files
- Add parameterized DAG runs via UI