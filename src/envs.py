import os
from urllib.parse import urlparse

# AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
# AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")
AIRFLOW_API_VERSION = os.getenv("AIRFLOW_API_VERSION", "v1")
MWAA_ENV_NAME = os.getenv("MWAA_ENV_NAME", "test")
AWS_PROFILE = os.getenv("AWS_PROFILE", "default")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
