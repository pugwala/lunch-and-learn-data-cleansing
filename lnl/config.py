"""Course settings. The instructor edits this file once and commits it; every notebook picks it up."""

# Everyone generates the identical dataset from these two numbers.
ROWS = 100_000
SEED = 2026

# ---- Databricks settings -----------------------------------------------------------------------
# A Unity Catalog volume the class can write to: the data is kept there between sessions, and session 5
# reads it directly with Spark. If the volume doesn't exist yet, everything still runs (data goes to /tmp).
# Set for Databricks Free Edition. For the agency workspace, change all three to the class's own catalog,
# schema and volume (e.g. "sandbox", "lunch_and_learn", "/Volumes/sandbox/lunch_and_learn/raw").
DATABRICKS_VOLUME = "/Volumes/workspace/default/lunch_and_learn"
# Catalog and schema where session 5 writes each person's cleansed Delta table.
DATABRICKS_CATALOG = "workspace"
DATABRICKS_SCHEMA = "default"

# Session 7 embedding model: a Hugging Face model name, or a folder path if the network blocks
# huggingface.co (download the model folder once, put it in a volume, and point this at it,
# e.g. "/Volumes/sandbox/lunch_and_learn/models/all-MiniLM-L6-v2").
EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# Sample size for the text sessions (spaCy and embeddings are slow on 100k notes on a laptop CPU).
TEXT_SAMPLE_SIZE = 10_000

# ---- Enterprise track (the notebooks in enterprise/, on the agency Databricks workspace) -----------------
# Mock data only, like the rest of the course. 00_preflight checks every one of these settings.
ENTERPRISE_CATALOG = ""            # a catalog you can create a schema in, e.g. "dmo_sandbox" (ask an admin)
ENTERPRISE_SCHEMA = "lnl_{user}"   # {user} becomes your user name, so each person gets their own schema
ENTERPRISE_VOLUME = "landing"      # created inside that schema: raw files land here, the pipeline reads them
ENTERPRISE_SERVERLESS = True       # serverless notebooks, jobs and pipelines; False = classic compute below
ENTERPRISE_CLUSTER_ID = ""         # classic only: an all-purpose cluster for the job's notebook task
ENTERPRISE_PIPELINE_POLICY_ID = "" # classic only: a cluster policy for the pipeline, if your admins require one
RESTRICTED_GROUP = ""              # account group allowed to see race and religion, e.g. "dmo-restricted"
ANALYST_GROUP = ""                 # account group that gets read access to the published view
ALERT_EMAILS = []                  # who gets job-failure email; empty = you
ALERT_DESTINATIONS = []            # display names of notification destinations an admin set up (PagerDuty, webhook)
