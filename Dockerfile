# Bake the pipeline dependencies into the image instead of installing them on
# every container start (_PIP_ADDITIONAL_REQUIREMENTS), so startup is fast and
# the dependency set is reproducible.
FROM apache/airflow:3.3.0-python3.11

ARG AIRFLOW_VERSION=3.3.0
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt --constraint "${CONSTRAINTS_URL}"
