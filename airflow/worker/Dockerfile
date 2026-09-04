# Worker-only image: the Celery worker is the only component that executes the
# Spark job (client mode, driver on the worker), so only it needs the full
# PySpark runtime. Connectors resolve at runtime via --packages.
FROM apache/airflow:3.3.1

# spark-submit needs a JVM on the driver host.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-17-openjdk-amd64/bin:${PATH}"

USER airflow

COPY requirements.txt requirements.txt
COPY requirements-worker.txt requirements-worker.txt

# PySpark wheels are large (~430 MB) and gitignored; fetch the pinned one once
# with download-pyspark-wheel.sh so image builds are fast and need no network.
COPY wheels wheels/

RUN pip install --find-links wheels --no-cache-dir -r requirements.txt -r requirements-worker.txt