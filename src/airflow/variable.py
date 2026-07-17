from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.variable_api import VariableApi

from src.airflow.airflow_client import get_api_client_and_host


def _variable_api(env_name: str, aws_profile: str, region: str) -> VariableApi:
    """Return a VariableApi bound to the requested MWAA target."""
    api_client, _ = get_api_client_and_host(env_name, aws_profile, region)
    return VariableApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (list_variables, "list_variables", "List all variables", True),
        (create_variable, "create_variable", "Create a variable", False),
        (get_variable, "get_variable", "Get a variable by key", True),
        (update_variable, "update_variable", "Update a variable by key", False),
        (delete_variable, "delete_variable", "Delete a variable by key", False),
    ]


async def list_variables(
    env_name: str,
    aws_profile: str,
    region: str,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    variable_api = _variable_api(env_name, aws_profile, region)
    # Build parameters dictionary
    kwargs: Dict[str, Any] = {}
    if limit is not None:
        kwargs["limit"] = limit
    if offset is not None:
        kwargs["offset"] = offset
    if order_by is not None:
        kwargs["order_by"] = order_by

    response = variable_api.get_variables(**kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def create_variable(
    env_name: str, aws_profile: str, region: str, key: str, value: str, description: Optional[str] = None
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    variable_api = _variable_api(env_name, aws_profile, region)
    variable_request = {
        "key": key,
        "value": value,
    }
    if description is not None:
        variable_request["description"] = description

    response = variable_api.post_variables(variable_request=variable_request)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def get_variable(
    env_name: str, aws_profile: str, region: str, key: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    variable_api = _variable_api(env_name, aws_profile, region)
    response = variable_api.get_variable(variable_key=key)
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def update_variable(
    env_name: str,
    aws_profile: str,
    region: str,
    key: str,
    value: Optional[str] = None,
    description: Optional[str] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    variable_api = _variable_api(env_name, aws_profile, region)
    update_request = {}
    if value is not None:
        update_request["value"] = value
    if description is not None:
        update_request["description"] = description

    response = variable_api.patch_variable(
        variable_key=key, update_mask=list(update_request.keys()), variable_request=update_request
    )
    return [types.TextContent(type="text", text=str(response.to_dict()))]


async def delete_variable(
    env_name: str, aws_profile: str, region: str, key: str
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    variable_api = _variable_api(env_name, aws_profile, region)
    response = variable_api.delete_variable(variable_key=key)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
