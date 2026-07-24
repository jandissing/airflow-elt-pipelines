# ELT Architecture Options

This document outlines different architectural patterns for integrating ELT services with Airflow orchestration, from simple monolithic to enterprise-scale distributed patterns.

---

## Current Architecture (Monolithic)

**Pattern:** ELT logic embedded in Airflow DAGs

```
Single Container/Repo:
├── airflow/
│   └── dags/
│       └── elt_pipeline_dag.py
├── elt/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── export.py
└── docker-compose.yml
```

**Characteristics:**
- ELT functions imported directly into DAG
- Runs in same Airflow container
- Shared dependencies and Python environment
- Single deployment unit

**Best For:**
- Learning/prototyping
- Small teams (1-3 people)
- Simple pipelines (1-5 tasks)
- Single deployment

**Limitations:**
- Tight coupling (changes to ELT require Airflow restart)
- Can't scale components independently
- Can't use different languages/tech stacks
- Limited fault isolation
- Harder to test independently

---

## Option 1: HTTP/REST API Pattern (Most Common)

**Pattern:** Airflow orchestrates external HTTP service

### Architecture
```
┌─────────────────────┐
│   Airflow Container │
│  (Orchestrator)     │
│                     │
│  DAG:               │
│  1. POST /jobs      │─────┐
│  2. GET /status     │     │ HTTP
│  3. GET /results    │     │
└─────────────────────┘     │
                            │
                    ┌───────▼────────────┐
                    │ ELT Service        │
                    │ (FastAPI/Flask)    │
                    │                    │
                    │ POST /jobs         │
                    │ GET /jobs/{id}     │
                    │ GET /jobs/{id}/... │
                    └────────────────────┘
```

### Implementation Example
**elt-service/app.py (Separate Repo)**
```python
from fastapi import FastAPI
from elt.extract import extract_raw_data
from elt.transform import transform_data
from elt.load import load_to_database

app = FastAPI()

@app.post("/api/v1/jobs")
async def start_job(job_type: str):
    job_id = str(uuid.uuid4())
    # Start async background task
    asyncio.create_task(run_job(job_id, job_type))
    return {"job_id": job_id}

@app.get("/api/v1/jobs/{job_id}/status")
async def get_status(job_id: str):
    return {"status": "running", "progress": 45}

@app.get("/api/v1/jobs/{job_id}/result")
async def get_result(job_id: str):
    return {"data": [...], "row_count": 1000}
```

**airflow/dags/elt_pipeline_dag.py (Separate Repo)**
```python
from airflow.operators.http import SimpleHttpOperator
from airflow.sensors.http import HttpSensor

start_job = SimpleHttpOperator(
    task_id="start_elt_job",
    http_conn_id="elt_service",
    endpoint="api/v1/jobs",
    method="POST",
    data={"job_type": "extract_transform_load"}
)

wait_for_completion = HttpSensor(
    task_id="wait_for_elt",
    http_conn_id="elt_service",
    endpoint="api/v1/jobs/{{ task_instance.xcom_pull('start_elt_job')['job_id'] }}/status",
    poke_interval=5,
)

fetch_results = SimpleHttpOperator(
    task_id="fetch_results",
    http_conn_id="elt_service",
    endpoint="api/v1/jobs/{{ task_instance.xcom_pull('start_elt_job')['job_id'] }}/result"
)
```

**Repository Structure**
```
airflow-repo/
├── dags/
│   └── elt_pipeline_dag.py (calls HTTP endpoints)
├── docker-compose.yml
└── README.md

elt-repo/
├── elt/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── export.py
├── app.py (FastAPI/Flask service)
├── requirements.txt
└── Dockerfile
```

**Pros:**
- ✅ Services fully decoupled
- ✅ Can use different languages (Python for ELT, Go for API, etc.)
- ✅ Independent scaling (add more ELT service replicas)
- ✅ Independent deployment/updates
- ✅ Clear separation of concerns
- ✅ Easy to test components independently
- ✅ Service discovery via service names or DNS

**Cons:**
- ❌ Network latency between services
- ❌ Increased complexity (service discovery, load balancing)
- ❌ Need to handle HTTP timeouts and retries
- ❌ Additional infrastructure (API service, load balancer)
- ❌ Requires monitoring/alerting on both services

**When to Use:**
- Growing teams (5+ people)
- Multiple DAGs reusing same ELT service
- Need different tech stacks for different stages
- Performance is critical (need independent scaling)

---

## Option 2: Message Queue Pattern

**Pattern:** Airflow pushes jobs to queue, ELT service consumes

### Architecture
```
┌──────────────────────┐
│  Airflow Container   │
│                      │
│  DAG:                │
│  1. Publish job ────→│
│  2. Subscribe result │
└──────────────────────┘
                │
                ▼
        ┌───────────────┐
        │  Message Queue│
        │ (RabbitMQ/    │
        │  Kafka/Redis) │
        └───────────────┘
                ▲
                │
┌────────────────┴──────────────────┐
│     ELT Service (Multiple          │
│     instances processing jobs)     │
└───────────────────────────────────┘
```

**Pros:**
- ✅ Async decoupled processing
- ✅ Built-in retry and error handling
- ✅ Multiple workers can process jobs in parallel
- ✅ Natural rate limiting and backpressure
- ✅ Durable (jobs don't get lost if service crashes)

**Cons:**
- ❌ Requires message broker infrastructure
- ❌ Complex debugging
- ❌ Eventual consistency (jobs might take time)

**When to Use:**
- Very high volume of jobs
- Long-running ETL tasks
- Need guaranteed delivery
- Multiple independent workers

---

## Option 3: Kubernetes Jobs Pattern

**Pattern:** Airflow submits Kubernetes jobs, K8s runs them on-demand

### Architecture
```
┌─────────────────────────┐
│  Airflow Pod            │
│  (Orchestrator in K8s)  │
│                         │
│  DAG:                   │
│  1. kubectl create job  │─┐
│  2. kubectl get status  │ │
│  3. kubectl logs        │ │
└─────────────────────────┘ │
                            │
    ┌───────────────────────┘
    │
    ▼
┌─────────────────────────┐
│  Kubernetes Cluster     │
│                         │
│  ┌────────────────────┐ │
│  │ ELT Job Pod (auto- │ │
│  │  created on demand)│ │
│  └────────────────────┘ │
│  ┌────────────────────┐ │
│  │ Another ELT Job    │ │
│  │ (auto-scaling)     │ │
│  └────────────────────┘ │
└─────────────────────────┘
```

**Implementation Example**
```python
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

extract_task = KubernetesPodOperator(
    task_id="extract",
    image="my-registry/elt-extract:latest",
    cmds=["python", "extract.py"],
    namespace="airflow",
    name="elt-extract-{{ ts_nodash }}"
)

transform_task = KubernetesPodOperator(
    task_id="transform",
    image="my-registry/elt-transform:latest",
    cmds=["python", "transform.py"],
    namespace="airflow",
    name="elt-transform-{{ ts_nodash }}"
)
```

**Pros:**
- ✅ Auto-scaling based on workload
- ✅ Resource efficiency (pods spin up/down on demand)
- ✅ Each job runs in isolated container
- ✅ Can use different Docker images per task
- ✅ Cloud-native and portable

**Cons:**
- ❌ Requires Kubernetes infrastructure
- ❌ Steeper learning curve
- ❌ Pod startup latency
- ❌ Requires container registry

**When to Use:**
- Already using Kubernetes
- Workloads are variable/bursty
- Need true isolation between jobs
- Want cloud-native architecture

---

## Option 4: Cloud Functions Pattern

**Pattern:** Airflow triggers serverless functions

### Architecture (AWS Example)
```
┌──────────────────────┐
│   Airflow DAG        │
│                      │
│   invoke_lambda()────┐
└──────────────────────┘
                       │
                       ▼
            ┌──────────────────┐
            │  AWS Lambda      │
            │  (Extract)       │
            └──────────────────┘
                       │
                       ▼
            ┌──────────────────┐
            │  AWS Lambda      │
            │  (Transform)     │
            └──────────────────┘
```

**Implementation Example**
```python
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator

extract_task = AwsLambdaInvokeFunctionOperator(
    task_id="extract",
    function_name="elt-extract-function",
    payload={"bucket": "raw-data"}
)

transform_task = AwsLambdaInvokeFunctionOperator(
    task_id="transform",
    function_name="elt-transform-function"
)
```

**Pros:**
- ✅ Fully managed, zero infrastructure
- ✅ Auto-scaling built-in
- ✅ Pay only for execution time
- ✅ No container orchestration needed
- ✅ Easy to deploy

**Cons:**
- ❌ Vendor lock-in (AWS/GCP/Azure specific)
- ❌ Cost unpredictable for large workloads
- ❌ Cold start latency
- ❌ Limited to function execution time limits

**When to Use:**
- Already on AWS/GCP/Azure
- Workloads fit function paradigm
- Want minimal operations overhead
- Not cost-sensitive for compute

---

## Option 5: dbt Pattern (Data Transformation Specific)

**Pattern:** Use dbt Cloud/Core for transformation, Airflow for orchestration

### Architecture
```
Airflow
├─ Extract (Python)
├─ dbt Transform (via dbt Cloud API)
│  └─ Returns job_id, polls status
└─ Load (Python)
```

**Implementation**
```python
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

dbt_transform = DbtCloudRunJobOperator(
    task_id="transform_with_dbt",
    job_id=123456,
    check_interval=10,
    timeout=3600
)
```

**Pros:**
- ✅ Purpose-built for data transformation
- ✅ Version control for transformations
- ✅ Testing and documentation built-in
- ✅ Great for SQL-based transforms

**Cons:**
- ❌ Limited to SQL transformations
- ❌ Additional platform (dbt Cloud) required
- ❌ Cost (if using Cloud)

**When to Use:**
- Primarily SQL transformations
- Want transformation version control
- Using dbt already

---

## Comparison Matrix

| Aspect | Monolithic | REST API | Message Queue | K8s Jobs | Cloud Functions | dbt |
|--------|-----------|----------|---------------|----------|-----------------|-----|
| Complexity | ⭐ Low | ⭐⭐⭐ Medium | ⭐⭐⭐⭐ High | ⭐⭐⭐⭐ High | ⭐⭐ Low-Med | ⭐⭐ Low-Med |
| Scalability | ❌ Poor | ✅ Good | ✅ Excellent | ✅ Excellent | ✅ Excellent | ⭐ SQL only |
| Decoupling | ❌ Tight | ✅ Good | ✅ Excellent | ✅ Good | ✅ Good | ⭐ Partial |
| Deployment | ✅ Simple | ⭐ Moderate | ❌ Complex | ❌ Complex | ✅ Simple | ✅ Simple |
| Cost | ✅ Low | ⭐ Moderate | ⭐ Moderate | ⭐ Variable | ⭐ Per-exec | ⭐ Platform fee |
| Monitoring | ⭐ Simple | ⭐⭐ Moderate | ❌ Complex | ⭐⭐ Moderate | ✅ Built-in | ✅ Built-in |
| Team Size | Small | Medium+ | Large | Medium+ | Any | Medium+ |

---

## Recommendation by Stage

### Stage 1: Prototype (You are here)
→ **Monolithic** (current setup)
- Fast iteration, minimal infrastructure

### Stage 2: MVP / Small Production
→ **HTTP/REST API Pattern** (recommended next step)
- Clear separation, easy to scale
- Single external service

### Stage 3: Scale-Up
→ **Kubernetes Jobs** or **Message Queue**
- High volume handling
- Auto-scaling capabilities

### Stage 4: Enterprise
→ **Multi-pattern**: K8s + Message Queue + Cloud Functions
- Different patterns for different workload types
- Fully decoupled, globally scalable

---

## Migration Path

```
Monolithic (Today)
        ↓
HTTP/REST API (Extract service to separate container)
        ↓
Kubernetes Jobs (Containerize each task separately)
        ↓
Message Queue + K8s (Add async queue for high volume)
        ↓
Cloud-Native (Lambda/Cloud Functions for components)
        ↓
Hybrid (Multiple patterns for different workloads)
```

Each migration step is gradual and doesn't require rewriting everything at once.

---

## Next Steps

To evolve from monolithic to REST API:

1. Create `elt-service/` repo with FastAPI
2. Move `elt/` package there
3. Expose endpoints (`/jobs`, `/jobs/{id}/status`, `/jobs/{id}/result`)
4. Update `airflow-repo/` DAGs to call HTTP endpoints
5. Deploy both services via Docker Compose or K8s
6. Test independent scaling

Would you like to implement **Option 1 (HTTP/REST API)** next?
