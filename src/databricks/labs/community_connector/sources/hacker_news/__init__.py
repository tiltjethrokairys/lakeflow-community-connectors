"""Hacker News source connector."""

from databricks.labs.community_connector.sources.hacker_news.hacker_news import (
    HackerNewsLakeflowConnect,
)

from databricks.labs.community_connector.sparkpds import LakeflowSource


class HackerNewsDataSource(LakeflowSource):
    _lakeflow_connect_cls = HackerNewsLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "hacker_news"


__all__ = [
    "HackerNewsLakeflowConnect",
    "HackerNewsDataSource",
]
