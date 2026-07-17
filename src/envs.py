import os

# The MWAA target (environment name, AWS profile, region) is supplied per tool call,
# not via environment variables, so a single server can serve multiple Airflows.
AIRFLOW_API_VERSION = os.getenv("AIRFLOW_API_VERSION", "v1")
