FROM apache/airflow:3.1.4

USER root
COPY requirements.txt /requirements.txt
RUN chown airflow:0 /requirements.txt && chmod 644 /requirements.txt

USER airflow

ARG AIRFLOW_VERSION=3.1.4
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    -r /requirements.txt

