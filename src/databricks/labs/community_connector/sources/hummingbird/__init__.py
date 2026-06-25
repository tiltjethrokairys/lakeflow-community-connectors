"""Hummingbird source connector."""

from databricks.labs.community_connector.sources.hummingbird.hummingbird import (
    HummingbirdLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class HummingbirdDataSource(LakeflowSource):
    _lakeflow_connect_cls = HummingbirdLakeflowConnect


__all__ = ["HummingbirdLakeflowConnect", "HummingbirdDataSource"]
