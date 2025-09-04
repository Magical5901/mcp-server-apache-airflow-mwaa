from urllib.parse import urljoin
import boto3
import requests

from airflow_client.client import ApiClient, Configuration

from src.envs import (
    AIRFLOW_API_VERSION,
    MWAA_ENV_NAME,
    AWS_PROFILE,
    AWS_REGION,
)


# Create boto3 session with profile and MWAA client
session = boto3.Session(profile_name=AWS_PROFILE)
mwaa_client = session.client("mwaa", region_name=AWS_REGION)
print(AWS_PROFILE)
print(AWS_REGION)


def create_mwaa_client_and_host(mwaa_name: str) -> tuple[ApiClient, str]:
    """Create MWAA API client and return both client and host URL."""
    try:
        response = mwaa_client.create_web_login_token(Name=mwaa_name)
        print(response)
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]

        host_url = f"https://{web_server_host_name}/api/{AIRFLOW_API_VERSION}"

        # Construct the URL needed for authentication
        login_url = f"https://{web_server_host_name}/aws_mwaa/login"
        login_payload = {"token": web_token}

        # Make a POST request to the MWAA login url using the login payload
        response = requests.post(login_url, data=login_payload, timeout=10)

        # Check if login was successful
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
        print(f"Request failed: {e!s}")
        raise e
    except Exception as e:
        print(f"An unexpected error occurred: {e!s}")
        raise e


api_client, AIRFLOW_HOST = create_mwaa_client_and_host(mwaa_name=MWAA_ENV_NAME)
print(AIRFLOW_HOST)
print(api_client.cookie)
