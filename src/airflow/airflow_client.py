import sys
from functools import lru_cache

import boto3
import requests
from airflow_client.client import ApiClient, Configuration

from src.envs import AIRFLOW_API_VERSION


def _create_api_client_and_host(env_name: str, aws_profile: str, region: str) -> tuple[ApiClient, str]:
    """Mint a web-login token for an MWAA environment and build an authenticated API client.

    Args:
        env_name: The MWAA environment name (e.g. "my-mwaa-environment").
        aws_profile: The local AWS profile used to call MWAA (credentials must be able to
            call ``mwaa:CreateWebLoginToken`` and be mapped to an Airflow RBAC role).
        region: The AWS region the environment lives in.

    Returns:
        A tuple of (authenticated ApiClient, Airflow REST API host URL).
    """
    session = boto3.Session(profile_name=aws_profile)
    mwaa_client = session.client("mwaa", region_name=region)
    print(f"MWAA env={env_name} profile={aws_profile} region={region}", file=sys.stderr)

    try:
        response = mwaa_client.create_web_login_token(Name=env_name)
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]

        host_url = f"https://{web_server_host_name}/api/{AIRFLOW_API_VERSION}"

        # Exchange the web login token for a session cookie.
        login_url = f"https://{web_server_host_name}/aws_mwaa/login"
        login_payload = {"token": web_token}
        response = requests.post(login_url, data=login_payload, timeout=10)

        if response.status_code == 200:
            configuration = Configuration(host=host_url)
            api_client = ApiClient(
                configuration=configuration,
                cookie=f"session={response.cookies['session']}",
            )
            return api_client, host_url
        else:
            raise RuntimeError(f"Failed to log in: HTTP: {response.status_code}")
    except requests.RequestException as e:
        print(f"Request failed: {e!s}", file=sys.stderr)
        raise e
    except Exception as e:
        print(f"An unexpected error occurred: {e!s}", file=sys.stderr)
        raise e


@lru_cache(maxsize=None)
def get_api_client_and_host(env_name: str, aws_profile: str, region: str) -> tuple[ApiClient, str]:
    """Return a cached authenticated API client and host for the given MWAA target.

    Authentication is lazy: the first call for a given (env_name, aws_profile, region)
    triple mints a web-login token and caches the resulting client, so a single running
    server can serve many MWAA environments — the caller (tool) picks the target per call.

    Note: the underlying MWAA session cookie eventually expires. If a long-lived server
    starts returning auth errors for a target, restart it to re-mint the token.
    """
    return _create_api_client_and_host(env_name=env_name, aws_profile=aws_profile, region=region)
