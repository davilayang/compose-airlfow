# Dockerfile to replicate Cloud Composer environment
FROM apache/airflow:1.10.14-python3.6

USER airflow

WORKDIR /opt/airflow

RUN python -m pip install --upgrade pip

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
