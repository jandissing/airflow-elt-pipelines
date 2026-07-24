# Local Airflow ELT Pipelines

A containerized data-engineering sandbox running **Apache Airflow 3.3.0** on
**PostgreSQL 15**, orchestrating two ELT pipelines with **pandas**. Everything
runs locally via Docker Compose.

- **`elt_pipeline_dag`** — forward ELT: reads a Postgres table, transforms it,
  loads the result back into another table, and exports a timestamped Excel file.
- **`csv_to_db_dag`** — reverse ELT: reads CSV files from `input/`, aggregates
  sales by region, and loads a summary table into Postgres.

---

## Stack

| Service     | Image                             | Host port | Purpose                                   |
|-------------|-----------------------------------|-----------|-------------------------------------------|
| `postgres`  | `postgres:15-alpine`              | **5433**  | Source data **and** Airflow metadata DB   |
| `webserver` | `apache/airflow:3.3.0-python3.11` | **8080**  | Airflow UI + REST/execution API server    |
| `scheduler` | `apache/airflow:3.3.0-python3.11` | –         | Schedules and runs tasks (LocalExecutor)  |

> **Note on the Postgres port:** the container listens on `5432`, but it is
> published to **`5433`** on your host to avoid clashing with any local Postgres.
> Inside the Docker network, services still connect via `postgres:5432`.

---

## Project structure

```
.
├── docker-compose.yml          # Service definitions (postgres, webserver, scheduler)
├── .env                        # Credentials & Airflow config (committed for local dev)
├── init-db.sql                 # Schema + sample data, runs on first DB startup
├── Makefile                    # Convenience commands
├── OPTIONS.md                  # Notes on scaling to REST/Celery/K8s architectures
│
├── dags/                       # Airflow DAG definitions (orchestration only)
│   ├── elt_pipeline_dag.py     #   forward ELT: DB → transform → DB + Excel
│   ├── csv_to_db_dag.py        #   reverse ELT: CSV → aggregate → DB
│   └── test_dag.py             #   minimal smoke-test DAG
│
├── elt/                        # Reusable pipeline logic (imported by the DAGs)
│   ├── config.py               #   DB connection string + table/dir constants
│   ├── extract_sales.py        #   ┐
│   ├── transform_sales.py      #   │ sales pipeline (elt_pipeline_dag)
│   ├── load_sales.py           #   │
│   ├── export_sales.py         #   ┘
│   ├── extract_csv.py          #   ┐
│   ├── transform_csv.py        #   │ csv pipeline (csv_to_db_dag)
│   └── load_csv.py             #   ┘
│
├── input/                      # CSV inputs for csv_to_db_dag
│   ├── sales_2024_01.csv
│   └── sales_2024_02.csv
├── output/                     # Generated Excel exports (git-ignored)
├── logs/                       # Airflow task logs (git-ignored)
└── plugins/                    # Custom Airflow plugins (empty)
```

**Design:** DAG files only wire tasks together; all real logic lives in the
`elt/` package as small, single-purpose modules (one flat package, module names
suffixed by pipeline: `*_sales` / `*_csv`). This keeps orchestration and logic
separate and makes the logic testable on its own.

---

## Prerequisites

- Docker & Docker Compose
- ~4 GB RAM available
- Host ports **8080** and **5433** free

---

## Quick start

```bash
# 1. Start all three services
docker-compose up -d

# 2. Watch them come up (first run pulls images + installs pip deps: ~1–2 min)
docker-compose ps        # wait until webserver & postgres are "healthy"

# 3. Open the UI
#    http://localhost:8080   →   login: admin / admin
```

On first startup the webserver runs `airflow db migrate`, creates the `admin`
user, and starts the API server; the scheduler then begins parsing DAGs.

> DAGs are **created paused** in Airflow 3.x — you must unpause a DAG (UI toggle
> or CLI) before it will run. See [Running the pipelines](#running-the-pipelines).

---

## The pipelines

### `elt_pipeline_dag` — forward ELT (DB → DB + Excel)

`schedule: 0 9 * * *` (daily 09:00) · `tags: elt, data-engineering`

```
extract ──▶ transform ──▶ ├─▶ load     (writes transformed_sales)
                          └─▶ export   (writes output/sales_report_<ts>.xlsx)
```

| Task        | Module               | What it does                                             |
|-------------|----------------------|---------------------------------------------------------|
| `extract`   | `extract_sales.py`   | `SELECT * FROM raw_sales_data`, push to XCom as JSON     |
| `transform` | `transform_sales.py` | Add `total_amount = quantity × unit_price`, select cols  |
| `load`      | `load_sales.py`      | Truncate + insert into `transformed_sales`               |
| `export`    | `export_sales.py`    | Write a timestamped `.xlsx` to `output/`, log summary    |

`load` and `export` both depend on `transform` and run in parallel.

### `csv_to_db_dag` — reverse ELT (CSV → DB)

`schedule: 0 8 * * *` (daily 08:00) · `tags: elt, csv, data-engineering`

```
extract_csv ──▶ aggregate_by_region ──▶ load_summary   (writes region_sales_summary)
```

| Task                  | Module            | What it does                                                        |
|-----------------------|-------------------|--------------------------------------------------------------------|
| `extract_csv`         | `extract_csv.py`  | Read & concat every `input/*.csv`, validate schema                 |
| `aggregate_by_region` | `transform_csv.py`| Group by region → total qty, total revenue, avg price, order count |
| `load_summary`        | `load_csv.py`     | Truncate + insert into `region_sales_summary`                      |

Drop more `*.csv` files (same columns as `raw_sales_data`) into `input/` and
re-run to include them.

---

## Running the pipelines

Because of Airflow 3.x behavior, use this flow:

```bash
# (once, after adding or editing a DAG) register it with the scheduler
docker-compose exec scheduler airflow dags reserialize

# unpause a DAG so it can run
docker-compose exec scheduler airflow dags unpause elt_pipeline_dag

# trigger a manual run
docker-compose exec scheduler airflow dags trigger elt_pipeline_dag
```

…or just flip the **pause toggle** and hit **Trigger** in the UI at
http://localhost:8080.

Check results:

```bash
# task states for the latest runs
docker-compose exec postgres psql -U airflow_user -d airflow_db \
  -c "SELECT dag_id, task_id, state FROM task_instance ORDER BY start_date DESC LIMIT 10;"

# Excel output
ls -lh output/
```

---

## Database

| Setting  | Value                    |
|----------|--------------------------|
| Host     | `localhost` (from host)  |
| Port     | **5433**                 |
| Database | `airflow_db`             |
| User     | `airflow_user`           |
| Password | `airflow_pass_2024`      |

In-container connection string (used by the DAGs):
`postgresql+psycopg2://airflow_user:airflow_pass_2024@postgres:5432/airflow_db`

Connect from your host:

```bash
psql -h localhost -p 5433 -U airflow_user -d airflow_db
```

### Tables (created by `init-db.sql`)

| Table                  | Written by            | Description                                   |
|------------------------|-----------------------|-----------------------------------------------|
| `raw_sales_data`       | seed data             | 8 sample sales rows (source for the sales DAG)|
| `transformed_sales`    | `elt_pipeline_dag`    | Per-row sales with `total_amount`             |
| `region_sales_summary` | `csv_to_db_dag`       | One row per region with aggregated metrics    |

> `init-db.sql` only runs the **first** time the Postgres volume is created. To
> re-seed from scratch, run `docker-compose down -v` (this deletes all data).

---

## Configuration

All settings live in **`.env`** (committed intentionally — these are throwaway
local-dev credentials, not secrets):

| Variable                        | Purpose                                    |
|---------------------------------|--------------------------------------------|
| `POSTGRES_USER/PASSWORD/DB`     | Postgres credentials & database name       |
| `AIRFLOW__CORE__FERNET_KEY`     | Encrypts Airflow connections/variables     |
| `AIRFLOW__WEBSERVER__SECRET_KEY`| Flask session signing key                  |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Airflow metadata DB connection       |

Python deps are installed at container start via `_PIP_ADDITIONAL_REQUIREMENTS`
(`psycopg2-binary openpyxl pandas sqlalchemy`) — fine for local dev; a real
deployment would bake these into a custom image instead.

---

## Make targets

```bash
make up          # start services
make down        # stop services
make status      # docker-compose ps
make logs        # tail webserver logs
make db-connect  # psql shell into the database
make db-query    # list tables + row counts
make trigger     # trigger elt_pipeline_dag
make clean       # remove logs/ and output/ files
make rebuild     # down -v + prune + up (full fresh start)
```

---

## Airflow 3.x notes & gotchas

This project targets Airflow **3.3.0**, which differs from the 2.x tutorials you
may find online. Things worth knowing:

- **Commands renamed:** `airflow db upgrade → db migrate`, `airflow webserver →
  api-server`. Health endpoint is `/api/v2/monitor/health`.
- **DAGs start paused** — unpause before they run.
- **DAG discovery can lag** — after adding a new DAG file, run
  `airflow dags reserialize` (or wait for the scheduler's parse cycle).
- **Local imports need the project root on `sys.path`.** Task workers don't
  inherit `PYTHONPATH`, so each DAG that imports the `elt` package prepends
  `/opt/airflow` to `sys.path` at the top of the file. If you add a brand-new
  local package and hit `ModuleNotFoundError`, **restart the scheduler**
  (`docker-compose restart scheduler`) — a first failed import can be cached for
  the life of the process.
- **Task execution API:** the scheduler's workers talk to the API server over
  HTTP; `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` points them at the webserver
  container. (If this is wrong, tasks fail with `Connection refused` and no logs.)

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| DAG not in the UI | Run `docker-compose exec scheduler airflow dags reserialize`; check `airflow dags list-import-errors`. |
| `ModuleNotFoundError: No module named 'elt'` | Restart the scheduler (stale failed-import cache); confirm the `sys.path` block is at the top of the DAG file. |
| Task fails instantly with `Connection refused` and no logs | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` misconfigured / webserver not healthy. |
| `port is already allocated` (5433/8080) | Another service owns the port. Stop it, or change the mapping in `docker-compose.yml`. |
| DAG runs pile up / “triggers every second” | A stray external trigger loop — the scheduler won’t self-trigger a paused DAG. Check for background scripts hitting `dags trigger`. |
| Need a clean slate | `docker-compose down -v && rm -rf logs/* output/* && docker-compose up -d` (deletes DB data). |

---

## Scaling beyond local

This runs a single-node **LocalExecutor** setup — ideal for development. For how
this evolves into distributed/production architectures (REST-API workers,
Celery + RabbitMQ/Redis, Kubernetes jobs, Cloud Functions, dbt), see
**[OPTIONS.md](OPTIONS.md)**.

---

## Next steps / ideas

- Externalize SQL into an `elt/sql/` folder loaded by the tasks.
- Add data-quality checks between transform and load.
- Incremental loads (track the last processed timestamp).
- Swap the warehouse: DuckDB / `fakesnow` locally, Snowflake in prod.
- Failure alerting (email/Slack) via Airflow callbacks.
