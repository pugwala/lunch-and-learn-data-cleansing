# Jupyter for the course notebooks and the lakehouse. Built from the repo root (see compose.yaml).
# The base image ships Java and Spark, so session 5 runs as-is. The same image runs Dagster, so every Python
# process in the stack has the same DuckDB.
# A dated docker-stacks build at least two weeks old, never latest: a rebuild must not bring new software.
ARG JUPYTER_BASE_TAG=2026-10-12
FROM quay.io/jupyter/pyspark-notebook:${JUPYTER_BASE_TAG}

# CPU-only PyTorch first. sentence-transformers (session 7) would otherwise pull the CUDA build: about 5 GB of
# NVIDIA libraries on a VM with no GPU. Installed before the course requirements so pip keeps this one.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch

# Course requirements: their own layer, so changing the lakehouse packages below doesn't reinstall these.
COPY requirements.txt /tmp/lnl-requirements.txt
COPY wheels/ /tmp/wheels/
RUN pip install --no-cache-dir -r /tmp/lnl-requirements.txt \
 && pip install --no-cache-dir /tmp/wheels/*.whl

# Lakehouse packages: DuckDB 1.5.x until the 2.0 upgrade phase, dbt, Dagster.
COPY lakehouse/jupyter/requirements-lake.txt /tmp/lake-requirements.txt
RUN pip install --no-cache-dir -r /tmp/lake-requirements.txt

# DuckDB extensions installed at build time, so a running container never needs the network for them.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres')"

WORKDIR /home/jovyan/course
