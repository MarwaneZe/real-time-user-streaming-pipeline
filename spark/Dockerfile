FROM apache/spark:4.2.0-python3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Job code baked in for a manual spark-submit fallback; the DAG path mounts it
# read-only at /opt/airflow/sparkjobs instead.
COPY jobs/ /opt/spark/jobs/

USER spark
