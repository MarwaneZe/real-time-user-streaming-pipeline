#!/usr/bin/env bash
# Fetch the prebuilt PySpark wheel used by the airflow-worker image build.
#
# The wheelhouse is gitignored (430 MB), so run this once before the first
# `docker compose up -d --build` to keep the worker build fast and offline.
set -euo pipefail

PYSPARK_VERSION="4.2.0"
WHEELHOUSE="$(cd "$(dirname "$0")" && pwd)/wheels"

mkdir -p "$WHEELHOUSE"
if [ -n "$(find "$WHEELHOUSE" -name "pyspark-${PYSPARK_VERSION}-*.whl" -print -quit)" ]; then
    echo "pyspark ${PYSPARK_VERSION} wheel already present in ${WHEELHOUSE}"
    exit 0
fi

echo "Downloading pyspark ${PYSPARK_VERSION} wheel to ${WHEELHOUSE}..."
python3 -m pip download --no-deps "pyspark==${PYSPARK_VERSION}" -d "$WHEELHOUSE"
echo "Done."