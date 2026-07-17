from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.config_api import ConfigApi

from src.airflow.airflow_client import get_api_client_and_host


def _config_api(env_name: str, aws_profile: str, region: str) -> ConfigApi:
    """Return a ConfigApi bound to the requested MWAA target."""
    api_client, _ = get_api_client_and_host(env_name, aws_profile, region)
    return ConfigApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_config, "get_config", "Get current configuration", True),
        (get_value, "get_value", "Get a specific option from configuration", True),
    ]


async def get_config(
    env_name: str,
    aws_profile: str,
    region: str,
    section: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    config_api = _config_api(env_name, aws_profile, region)
    # Build parameters dictionary
    kwargs: Dict[str, Any] = {}
    if section is not None:
        kwargs["section"] = section

    response = config_api.get_config(**kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_value(
    env_name: str, aws_profile: str, region: str, section: str, option: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    config_api = _config_api(env_name, aws_profile, region)
    response = config_api.get_value(section=section, option=option)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
