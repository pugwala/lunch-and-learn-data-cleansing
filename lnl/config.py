"""Course settings. The instructor edits this file once and commits it; every notebook picks it up.

REPO_URL also appears in each notebook's setup cell (Colab needs it before this package exists).
Run  python tools/set_repo.py OWNER/REPO  to update every copy at once.
"""
REPO_URL = "https://github.com/YOUR-ORG/lunch-and-learn-data-cleansing"

# Everyone generates the identical dataset from these two numbers.
ROWS = 100_000
SEED = 2026

# ---- Databricks only (recommended). Leave blank to fall back to local files. -----------------
# A Unity Catalog volume the class can write to. Spark on serverless or shared compute can only
# read files that live in a volume, so session 5 needs this on those compute types.
DATABRICKS_VOLUME = ""    # e.g. "/Volumes/sandbox/lunch_and_learn/raw"; Free Edition: "/Volumes/workspace/default/lunch_and_learn"
# Catalog and schema where session 5 writes each person's cleansed Delta table.
DATABRICKS_CATALOG = ""   # e.g. "sandbox"; Free Edition: "workspace"
DATABRICKS_SCHEMA = ""    # e.g. "lunch_and_learn"; Free Edition: "default"

# Session 7 embedding model: a Hugging Face model name, or a folder path if the network blocks
# huggingface.co (download the model folder once, put it in a volume, and point this at it,
# e.g. "/Volumes/sandbox/lunch_and_learn/models/all-MiniLM-L6-v2").
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Sample size for the text sessions (spaCy and embeddings are slow on 100k notes on a laptop CPU).
TEXT_SAMPLE_SIZE = 10_000
