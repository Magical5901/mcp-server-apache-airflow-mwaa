from typing import Callable, List, Union

import mcp.types as types
from airflow_client.client.api.monitoring_api import MonitoringApi

from src.airflow.airflow_client import get_api_client_and_host


def _monitoring_api(env_name: str, aws_profile: str, region: str) -> MonitoringApi:
    """Return a MonitoringApi bound to the requested MWAA target."""
    api_client, _ = get_api_client_and_host(env_name, aws_profile, region)
    return MonitoringApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_health, "get_health", "Get instance status", True),
        (get_version, "get_version", "Get version information", True),
    ]


async def get_health(
    env_name: str, aws_profile: str, region: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """
    Get the status of Airflow's metadatabase, triggerer and scheduler.
    It includes info about metadatabase and last heartbeat of scheduler and triggerer.
    """
    monitoring_api = _monitoring_api(env_name, aws_profile, region)
    response = monitoring_api.get_health()
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_version(
    env_name: str, aws_profile: str, region: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """
    Get version information about Airflow.
    """
    monitoring_api = _monitoring_api(env_name, aws_profile, region)
    response = monitoring_api.get_version()
    return [types.TextContent(type="text", text=str(response.to_dict()))]
