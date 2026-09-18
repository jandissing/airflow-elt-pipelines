# Local Airflow ELT Pipelines

A containerized data-engineering sandbox running **Apache Airflow 3.3.0** on
**PostgreSQL 15**, orchestrating two ELT pipelines with **pandas**. Everything
runs locally via Docker Compose, with unit tests and CI.

- **`elt_pipeline_dag`** — forward ELT: reads a Postgres table, transforms it,
  loads the result back into another table, and exports a timestamped Excel file.
- **`csv_to_db_dag`** — reverse ELT: reads CSV files from `input/`, aggregates
  sales by region, and loads a summary table into Postgres.

---

## Stack

| Service     | Image                                | Host port | Purpose                                   |
|-------------|--------------------------------------|-----------|-------------------------------------------|
| `postgres`  | `postgres:15-alpine`                 | **5433**  | Source data **and** Airflow metadata DB   |
| `webserver` | built from `Dockerfile` (Airflow 3.3)| **8080**  | Airflow UI + REST/execution API server    |
| `scheduler` | built from `Dockerfile` (Airflow 3.3)| –         | Schedules and runs tasks (LocalExecutor)  |

> **Note on the Postgres port:** the container listens on `5432`, but it is
> published to **`5433`** on your host to avoid clashing with any local Postgres.
> Inside the Docker network, services still connect via `postgres:5432`.

---

## Project structure

```
.
├── docker-compose.yml          # Service definitions (postgres, webserver, scheduler)
├── Dockerfile                  # Airflow image + pinned pipeline dependencies
├── requirements.txt            # Runtime deps baked into the image
├── requirements-dev.txt        # Test/lint deps (adds Airflow, pytest, ruff)
├── .env.example                # Template for .env (real .env is git-ignored)
├── init-db.sql                 # Schema + sample data, runs on first DB startup
├── Makefile                    # Convenience commands
├── pyproject.toml              # pytest + ruff configuration
├── OPTIONS.md                  # Notes on scaling to REST/Celery/K8s architectures
│
├── dags/                       # Airflow DAG definitions (orchestration only)
│   ├── elt_pipeline_dag.py     #   forward ELT: DB → transform → DB + Excel
│   ├── csv_to_db_dag.py        #   reverse ELT: CSV → aggregate → DB
│   └── test_dag.py             #   minimal smoke-test DAG
│
├── elt/                        # Reusable pipeline logic (imported by the DAGs)
│   ├── config.py               #   DB connection builder + table/dir constants
│   ├── extract_sales.py        #   ┐
│   ├── transform_sales.py      #   │ sales pipeline (elt_pipeline_dag)
│   ├── load_sales.py           #   │
│   ├── export_sales.py         #   ┘
│   ├── extract_csv.py          #   ┐
│   ├── transform_csv.py        #   │ csv pipeline (csv_to_db_dag)
│   └── load_csv.py             #   ┘
│
├── tests/                      # pytest suite (see "Tests" below)
├── .github/workflows/ci.yml    # Lint, tests, DAG import check, image build
├── input/                      # CSV inputs for csv_to_db_dag
├── output/                     # Generated Excel exports (git-ignored)
├── logs/                       # Airflow task logs (git-ignored)
└── plugins/                    # Custom Airflow plugins (empty)
```

**Design:** DAG files only wire tasks together; all real logic lives in the
`elt/` package as small, single-purpose modules (one flat package, module names
suffixed by pipeline: `*_sales` / `*_csv`). Each module splits in two:

- a **pure function** that takes and returns a DataFrame (`summarize_by_region`,
  `transform_sales_frame`, `read_csv_directory`, `write_excel_report`,
  `normalize_sale_date`) — no Airflow, unit-tested directly;
- a **task callable** (`aggregate_by_region(**context)`, …) that does the XCom
  push/pull and logging around it.

That split is what makes the business logic testable without an Airflow runtime.

---

## Prerequisites

- Docker & Docker Compose v2
- ~4 GB RAM available
- Host ports **8080** and **5433** free

---

## Quick start

### 1. Create your `.env`

The stack reads its credentials from a `.env` file, which is **not** in the
repository. Copy the template:

```bash
cp .env.example .env     # or: make env
```

Now open `.env` and replace the four placeholder values. All three secrets can
be generated with the standard library — no extra packages needed:

```bash
# AIRFLOW__CORE__FERNET_KEY  (must be a 32-byte urlsafe-base64 key, not any random string)
python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"

# AIRFLOW__API__SECRET_KEY and AIRFLOW__API_AUTH__JWT_SECRET (run twice, use different values)
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

> ⚠️ **`POSTGRES_PASSWORD` appears twice** — on its own line *and* inside
> `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`. The two must match, or Airflow starts
> but can't reach its metadata database.

Nothing else in `.env` needs changing for a local run.

### 2. Start the stack

```bash
docker compose up -d --build     # or: make up

docker compose ps                # wait until webserver & postgres are "healthy"
                                 # (first run pulls the base image: ~1-2 min)
```

### 3. Open the UI

http://localhost:8080 — log in as `admin` with the password you set in
`AIRFLOW_ADMIN_PASSWORD` (default: `admin`).

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
|-------------|----------------------|----------------------------------------------------------|
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
|-----------------------|-------------------|---------------------------------------------------------------------|
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
docker compose exec scheduler airflow dags reserialize

# unpause a DAG so it can run
docker compose exec scheduler airflow dags unpause elt_pipeline_dag

# trigger a manual run
docker compose exec scheduler airflow dags trigger elt_pipeline_dag
```

…or `make trigger` / `make trigger-csv`, or flip the **pause toggle** and hit
**Trigger** in the UI at http://localhost:8080.

Check results:

```bash
make db-query          # table list + row counts
ls -lh output/         # Excel output
```

---

## Tests

The suite runs without Docker. Pure-function tests need only pandas; the DAG
tests need Airflow installed and are **skipped automatically** if it isn't.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.11.txt

pytest -q        # or: make test
ruff check .     # or: make lint
```

| File                       | Covers                                                            |
|----------------------------|-------------------------------------------------------------------|
| `test_transform_csv.py`    | Region aggregation: sums, averages, rounding, no input mutation   |
| `test_transform_sales.py`  | `total_amount` maths, output columns, missing-column failure      |
| `test_extract_csv.py`      | Multi-file concat, non-CSV files ignored, schema/empty-dir errors |
| `test_export_sales.py`     | Workbook round-trips, timestamped filename, dir auto-created      |
| `test_load_sales.py`       | `sale_date` coercion after the XCom JSON round-trip               |
| `test_config.py`           | Connection string from env; refuses to run without a password     |
| `test_pipeline_wiring.py`  | XCom keys and `task_ids` line up across tasks (uses a fake TI)    |
| `test_dags.py`             | DAG files import cleanly; task dependencies and retries are set   |

`pyproject.toml` turns `FutureWarning` into an error, so a deprecated pandas
call fails the build instead of quietly piling up in task logs.

CI (`.github/workflows/ci.yml`) runs ruff + pytest on Python 3.11 against the
official Airflow constraints, and separately validates the compose file and
builds the image.

---

## Database

| Setting  | Value                                  |
|----------|----------------------------------------|
| Host     | `localhost` (from host)                |
| Port     | **5433**                               |
| Database | `$POSTGRES_DB` (default `airflow_db`)  |
| User     | `$POSTGRES_USER` (default `airflow_user`) |
| Password | `$POSTGRES_PASSWORD` (set in `.env`)   |

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
> re-seed from scratch, run `docker compose down -v` (this deletes all data).

---

## Configuration

Settings live in **`.env`**, which is **git-ignored**; `.env.example` is the
committed template. Nothing in the code has a credential default — `elt/config.py`
raises if `POSTGRES_PASSWORD` is unset rather than silently connecting somewhere.

| Variable                              | Purpose                                      |
|---------------------------------------|----------------------------------------------|
| `POSTGRES_USER/PASSWORD/DB`           | Postgres credentials & database name         |
| `DB_HOST` / `DB_PORT`                 | Where the pipelines reach the warehouse      |
| `AIRFLOW__CORE__FERNET_KEY`           | Encrypts Airflow connections/variables       |
| `AIRFLOW__API__SECRET_KEY`            | Session signing key (also set as the legacy `[webserver]` key for FAB) |
| `AIRFLOW__API_AUTH__JWT_SECRET`       | Signs the task-execution API tokens          |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Airflow metadata DB connection               |
| `AIRFLOW_ADMIN_PASSWORD`              | Password for the `admin` UI user             |

Python dependencies are **baked into the image** (`Dockerfile` + `requirements.txt`),
pinned to the official Airflow 3.3.0 constraints file — so container startup is
fast and the dependency tree is reproducible.

---

## Make targets

```bash
make env         # create .env from .env.example
make up          # build + start services
make down        # stop services
make status      # container status
make logs        # tail webserver logs
make test        # pytest
make lint        # ruff
make validate    # DAG import errors + DAG list
make trigger     # unpause + trigger elt_pipeline_dag
make trigger-csv # unpause + trigger csv_to_db_dag
make db-connect  # psql shell into the database
make db-query    # list tables + row counts
make clean       # remove logs/ and output/ files
make rebuild     # down -v + rebuild image + up (full fresh start)
```

---

## Design notes & trade-offs

Deliberate choices for a local demo that would change in production:

- **DataFrames travel through XCom as JSON.** Fine for these row counts; at real
  volume this bloats the metadata database. Production would pass object-storage
  paths (or a staging table) between tasks, or use a custom XCom backend.
- **Plain SQLAlchemy engines, credentials from env** rather than Airflow
  Connections / `PostgresHook`. Keeps the repo runnable with zero UI setup; a
  real deployment would store the connection in Airflow (encrypted by the Fernet
  key) or a secrets backend.
- **The warehouse shares the Airflow metadata database.** One container instead
  of two; a real system keeps operational metadata and analytical data apart.
- **Loads are truncate-and-replace.** Simple and idempotent at this size;
  incremental/upsert loads are the next step (see below).

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
  (`docker compose restart scheduler`) — a first failed import can be cached for
  the life of the process.
- **Task execution API:** the scheduler's workers talk to the API server over
  HTTP; `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` points them at the webserver
  container. (If this is wrong, tasks fail with `Connection refused` and no logs.)
- **`DagBag` changed:** it no longer takes `include_examples`, and `get_dag()`
  reads from the metadata DB — tests use `DagBag(dag_folder="dags").dags[...]`.

---

## Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| `required variable POSTGRES_PASSWORD is missing` on `docker compose up` | No `.env` yet — run `cp .env.example .env` (or `make env`) and fill it in. |
| DAG not in the UI | Run `docker compose exec scheduler airflow dags reserialize`; check `airflow dags list-import-errors`. |
| `ModuleNotFoundError: No module named 'elt'` | Restart the scheduler (stale failed-import cache); confirm the `sys.path` block is at the top of the DAG file. |
| Task fails instantly with `Connection refused` and no logs | `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` misconfigured / webserver not healthy. |
| `RuntimeError: POSTGRES_PASSWORD is not set` in a task log | The Airflow services aren't getting the warehouse env vars — check `.env` and restart. |
| `port is already allocated` (5433/8080) | Another service owns the port. Stop it, or change the mapping in `docker-compose.yml`. |
| Need a clean slate | `make rebuild` (deletes the DB volume). |

---

## Scaling beyond local

This runs a single-node **LocalExecutor** setup — ideal for development. For how
this evolves into distributed/production architectures (REST-API workers,
Celery + RabbitMQ/Redis, Kubernetes jobs, Cloud Functions, dbt), see
**[OPTIONS.md](OPTIONS.md)**.

---

## Next steps / ideas

- Externalize SQL into an `elt/sql/` folder loaded by the tasks.
- Data-quality checks between transform and load (row counts, null ratios).
- Incremental loads (track the last processed timestamp) instead of truncate+insert.
- Failure alerting (email/Slack) via Airflow callbacks, and SLAs on the tasks.
- Swap the warehouse: DuckDB / `fakesnow` locally, Snowflake in prod.
