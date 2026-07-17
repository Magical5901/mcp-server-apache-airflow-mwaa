from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.import_error_api import ImportErrorApi

from src.airflow.airflow_client import get_api_client_and_host


def _import_error_api(env_name: str, aws_profile: str, region: str) -> ImportErrorApi:
    """Return an ImportErrorApi bound to the requested MWAA target."""
    api_client, _ = get_api_client_and_host(env_name, aws_profile, region)
    return ImportErrorApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_import_errors, "get_import_errors", "List import errors", True),
        (get_import_error, "get_import_error", "Get a specific import error by ID", True),
    ]


async def get_import_errors(
    env_name: str,
    aws_profile: str,
    region: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    import_error_api = _import_error_api(env_name, aws_profile, region)
    # Build parameters dictionary
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = import_error_api.get_import_errors(**kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_import_error(
    env_name: str,
    aws_profile: str,
    region: str,
    import_error_id: int,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    import_error_api = _import_error_api(env_name, aws_profile, region)
    response = import_error_api.get_import_error(import_error_id=import_error_id)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
