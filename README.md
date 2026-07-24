# Local Airflow + PostgreSQL ELT Pipeline

A containerized Extract-Load-Transform (ELT) data pipeline using Apache Airflow and PostgreSQL.

## Prerequisites

- **Docker** and **Docker Compose** installed
- Minimum 4GB RAM available
- Ports **5432** (PostgreSQL) and **8080** (Airflow UI) must be available

## Quick Start

### 1. Create Project Structure

```bash
mkdir -p dags logs plugins output
```

### 2. Verify File Placement

Ensure the following files are in the project root:
- `docker-compose.yml`
- `init-db.sql`
- `.env`
- `README.md` (this file)
- `Makefile`
- `.gitignore`
- `dags/elt_pipeline_dag.py`

### 3. Start Services

```bash
docker-compose up -d
```

### 4. Wait for Startup

Services take 30-60 seconds to start. Check status:

```bash
docker-compose ps
```

Wait until all containers show `healthy` or `running` status.

### 5. Access Airflow UI

Open your browser and navigate to:

```
http://localhost:8080
```

**Login Credentials:**
- Username: `admin`
- Password: `admin`

## Running the DAG

### Manual Trigger (UI)

1. Open http://localhost:8080
2. Find `elt_pipeline_dag` in the DAG list
3. Click the **Trigger DAG** button
4. Wait for all tasks to complete (should take 1-2 minutes)

### Manual Trigger (CLI)

```bash
docker-compose exec webserver airflow dags trigger elt_pipeline_dag
```

### View Output Files

Generated Excel files are saved in the `output/` directory:

```bash
ls -lh output/
```

Files are named: `sales_report_YYYYMMDD_HHMMSS.xlsx`

## Pipeline Breakdown

### Task 1: Extract
- Reads all 8 rows from `raw_sales_data` table
- Orders results by `sale_date`
- Pushes raw data to XCom for next task

### Task 2: Transform
- Pulls raw data from Extract task
- Calculates `total_amount = quantity × unit_price`
- Selects relevant columns for final dataset
- Pushes transformed data to XCom

### Task 3: Load (Parallel with Export)
- Pulls transformed data from Transform task
- Clears existing data from `transformed_sales` table
- Inserts newly transformed records
- Logs row count

### Task 4: Export (Parallel with Load)
- Pulls transformed data from Transform task
- Creates timestamped Excel file
- Auto-adjusts column widths
- Logs summary statistics: total records, total revenue, revenue by region

## Database Details

### Connection String

```
postgresql+psycopg2://airflow_user:airflow_pass_2024@postgres:5432/airflow_db
```

### Connection Parameters

- **Host:** `postgres` (or `localhost` from host machine)
- **Port:** `5432`
- **Database:** `airflow_db`
- **User:** `airflow_user`
- **Password:** `airflow_pass_2024` (from `.env`)

### Table Schemas

#### raw_sales_data
```
Column        | Type              | Notes
------------- | ----------------- | -----
id            | SERIAL PRIMARY KEY| Auto-increment
customer_name | VARCHAR(255)      | Customer name
product_name  | VARCHAR(255)      | Product name
quantity      | INTEGER           | Number of units
unit_price    | DECIMAL(10,2)     | Price per unit
sale_date     | DATE              | Transaction date
region        | VARCHAR(50)       | Geographic region
```

#### transformed_sales
```
Column         | Type              | Notes
-------------- | ----------------- | -----
id             | SERIAL PRIMARY KEY| Auto-increment
customer_name  | VARCHAR(255)      | Customer name
product_name   | VARCHAR(255)      | Product name
total_amount   | DECIMAL(10,2)     | quantity × unit_price
sale_date      | DATE              | Transaction date
region         | VARCHAR(50)       | Geographic region
transformed_at | TIMESTAMP         | Processing timestamp
```

### Connect from Host Machine

```bash
psql -h localhost -U airflow_user -d airflow_db
# Enter password: airflow_pass_2024
```

View tables:
```sql
\dt
SELECT COUNT(*) FROM transformed_sales;
```

## Useful Commands

### View Logs

**Webserver logs:**
```bash
docker-compose logs -f webserver
```

**Scheduler logs:**
```bash
docker-compose logs -f scheduler
```

**All logs:**
```bash
docker-compose logs -f
```

### Stop Services

```bash
docker-compose down
```

### Trigger DAG Manually

```bash
docker-compose exec webserver airflow dags trigger elt_pipeline_dag
```

### Validate DAG Syntax

```bash
docker-compose exec webserver airflow dags test elt_pipeline_dag
```

### Fresh Restart (Remove Data)

```bash
docker-compose down -v
rm -rf logs/ output/
docker-compose up -d
```

### Check Container Status

```bash
docker-compose ps
```

## Customization

### Adding More Sample Data

Edit `init-db.sql` and add more rows to the `INSERT INTO raw_sales_data` statement before running `docker-compose up`.

### Modifying DAG Logic

1. Edit `dags/elt_pipeline_dag.py`
2. Save changes (Airflow monitors the file)
3. Refresh the UI (F5)
4. The updated DAG will appear automatically

### Changing Schedule

Edit `dags/elt_pipeline_dag.py`, line ~30:

```python
schedule_interval="0 9 * * *",  # Change to different cron expression
```

Cron format: `minute hour day month weekday`
- `0 9 * * *` = Every day at 9 AM
- `0 */6 * * *` = Every 6 hours
- `0 0 * * 0` = Every Sunday at midnight

### Auto-Reload Behavior

Airflow monitors the `dags/` folder and automatically detects changes. No restart needed for DAG logic changes.

## Output Files

### Location
```
output/
```

### Naming Convention
```
sales_report_YYYYMMDD_HHMMSS.xlsx
```

Example: `sales_report_20240122_143052.xlsx`

### Format
- Single sheet named "Sales Data"
- Headers in first row
- Auto-adjusted column widths (max 50 characters)
- No data overwrites (each run creates new file)

## Troubleshooting

### Connection Refused (Port 5432)

**Problem:** `Cannot connect to postgres:5432`

**Solution:**
```bash
# Check if port is in use
lsof -i :5432

# Stop any conflicting services
docker-compose down -v

# Restart
docker-compose up -d
```

### DAG Doesn't Appear in UI

**Problem:** `elt_pipeline_dag` not visible after 2 minutes

**Solution:**
1. Check for syntax errors:
   ```bash
   docker-compose exec webserver python -m py_compile dags/elt_pipeline_dag.py
   ```
2. Check logs:
   ```bash
   docker-compose logs webserver | grep error
   ```
3. Wait 1-2 minutes for DAG to parse
4. Refresh browser (F5)

### Memory Issues

**Problem:** Containers exit or system becomes slow

**Solution:**
- Ensure 4GB+ RAM available
- Close unnecessary applications
- Run: `docker system prune` to free up space

### Task Failures in DAG

**Problem:** Tasks show red in UI

**Solution:**
1. Click on failed task
2. View logs for error details
3. Check database connectivity
4. Verify credentials in `.env`
5. Ensure `transformed_sales` table exists

## Next Steps

### Incremental Loads
- Track last execution timestamp
- Only process new records since last run
- Modify transform query with WHERE clause

### Data Quality Checks
- Add SQL assertions before load
- Implement row counts validation
- Check for NULL values in critical columns

### Alerts & Monitoring
- Configure email notifications for failures
- Set up Slack integration for task completion
- Monitor task runtime performance

### Advanced Transformations
- Integrate dbt for complex transformations
- Add multiple transformation stages
- Implement slowly changing dimensions

---

**Version:** 1.0  
**Last Updated:** 2024-01-22
