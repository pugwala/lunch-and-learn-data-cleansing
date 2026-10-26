# Jupyter for the course notebooks and the lakehouse. Built from the repo root (see compose.yaml).
# The base image ships Java and Spark, so session 5 runs as-is. The same image runs Dagster, so every Python
# process in the stack has the same DuckDB.
# A dated docker-stacks build at least two weeks old, never latest: a rebuild must not bring new software.
ARG JUPYTER_BASE_TAG=2026-10-12
FROM quay.io/jupyter/pyspark-notebook:${JUPYTER_BASE_TAG}

# pip resolves as of this date (--uploaded-prior-to), the base image's own: the course's open-ended requirements and
# every transitive dependency are what existed then, never what was uploaded the day of a rebuild. A build ARG, not
# ENV, so the course's setup cells can still install what they need at run time.
ARG PIP_CUTOFF=2026-10-12

# CPU-only PyTorch first. sentence-transformers (session 7) would otherwise pull the CUDA build: about 5 GB of
# NVIDIA libraries on a VM with no GPU. Installed before the course requirements so pip keeps this one.
# PyTorch's index doesn't publish upload times, so the cutoff can't apply there: torch names its version (2026-09-30).
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.14.1

# Course requirements: their own layer, so changing the lakehouse packages below doesn't reinstall these.
COPY requirements.txt /tmp/lnl-requirements.txt
COPY wheels/ /tmp/wheels/
RUN pip install --no-cache-dir --uploaded-prior-to "$PIP_CUTOFF" -r /tmp/lnl-requirements.txt \
 && pip install --no-cache-dir /tmp/wheels/*.whl

# Lakehouse packages: DuckDB 1.5.x until the 2.0 upgrade phase, dbt, Dagster.
COPY lakehouse/jupyter/requirements-lake.txt /tmp/lake-requirements.txt
RUN pip install --no-cache-dir --uploaded-prior-to "$PIP_CUTOFF" -r /tmp/lake-requirements.txt

# DuckDB extensions installed at build time, so a running container never needs the network for them.
RUN python -c "import duckdb; c = duckdb.connect(); c.execute('INSTALL ducklake'); c.execute('INSTALL postgres')"

WORKDIR /home/jovyan/course
