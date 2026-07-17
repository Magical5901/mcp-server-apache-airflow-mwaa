from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.provider_api import ProviderApi

from src.airflow.airflow_client import get_api_client_and_host


def _provider_api(env_name: str, aws_profile: str, region: str) -> ProviderApi:
    """Return a ProviderApi bound to the requested MWAA target."""
    api_client, _ = get_api_client_and_host(env_name, aws_profile, region)
    return ProviderApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_providers, "get_providers", "Get a list of loaded providers", True),
    ]


async def get_providers(
    env_name: str,
    aws_profile: str,
    region: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    """
    Get a list of providers.

    Args:
        limit: The numbers of items to return.
        offset: The number of items to skip before starting to collect the result set.

    Returns:
        A list of providers with their details.
    """
    provider_api = _provider_api(env_name, aws_profile, region)
    # Build parameters dictionary
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset

    response = provider_api.get_providers(**kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
