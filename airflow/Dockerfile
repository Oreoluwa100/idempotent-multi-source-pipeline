FROM apache/airflow:2.10.5-python3.11

USER root
RUN python -m venv /opt/project_venv && chown -R airflow:root /opt/project_venv

USER airflow
RUN PIP_USER=false /opt/project_venv/bin/pip install --no-cache-dir \
        google-cloud-bigquery \
        psycopg2-binary \
        python-dotenv \
        requests \
        dbt-core==1.12.5 \
        dbt-bigquery==1.12.1