from typing import Any, Callable, Dict, List, Optional, Union

import mcp.types as types
from airflow_client.client.api.dag_stats_api import DagStatsApi

from src.airflow.airflow_client import get_api_client_and_host


def _dag_stats_api(env_name: str, aws_profile: str, region: str) -> DagStatsApi:
    """Return a DagStatsApi bound to the requested MWAA target."""
    api_client, _ = get_api_client_and_host(env_name, aws_profile, region)
    return DagStatsApi(api_client)


def get_all_functions() -> list[tuple[Callable, str, str, bool]]:
    """Return list of (function, name, description, is_read_only) tuples for registration."""
    return [
        (get_dag_stats, "get_dag_stats", "Get DAG stats", True),
    ]


async def get_dag_stats(
    env_name: str,
    aws_profile: str,
    region: str,
    dag_ids: Optional[List[str]] = None,
) -> List[Union[types.TextContent, types.ImageContent, types.EmbeddedResource]]:
    dag_stats_api = _dag_stats_api(env_name, aws_profile, region)
    # Build parameters dictionary
    kwargs: Dict[str, Any] = {}
    if dag_ids is not None:
        kwargs["dag_ids"] = dag_ids

    response = dag_stats_api.get_dag_stats(**kwargs)
    return [types.TextContent(type="text", text=str(response.to_dict()))]
