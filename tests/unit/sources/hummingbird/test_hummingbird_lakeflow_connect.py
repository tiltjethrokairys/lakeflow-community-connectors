"""Offline structural tests for the Hummingbird connector.

These run with no credentials and no network — they validate the static
surface (tables/schemas/metadata) and the offset-convergence contract. The full
live read path is exercised via ``tools/scripts/stream_to_files_local.py`` and,
as a follow-up, a ``source_simulator`` corpus driving ``LakeflowConnectTests``.
"""

import pytest
from pyspark.sql.types import StructType

from databricks.labs.community_connector.sources.hummingbird.hummingbird import (
    HummingbirdLakeflowConnect,
)

DUMMY = {"client_id": "x", "client_secret": "y"}


@pytest.fixture
def connector():
    return HummingbirdLakeflowConnect(dict(DUMMY))


def test_requires_credentials(monkeypatch):
    monkeypatch.delenv("HUMMINGBIRD_CLIENT_ID", raising=False)
    monkeypatch.delenv("HUMMINGBIRD_CLIENT_SECRET", raising=False)
    with pytest.raises(ValueError):
        HummingbirdLakeflowConnect({})


def test_list_tables(connector):
    assert sorted(connector.list_tables()) == ["cases", "history_events"]


def test_schemas_deterministic(connector):
    for table in connector.list_tables():
        s1 = connector.get_table_schema(table, {})
        s2 = connector.get_table_schema(table, {})
        assert isinstance(s1, StructType) and s1.fields
        assert s1 == s2


def test_metadata_consistent_with_schema(connector):
    for table in connector.list_tables():
        meta = connector.read_table_metadata(table, {})
        assert meta["ingestion_type"] == "cdc"
        assert meta["primary_keys"] == ["token"]
        names = connector.get_table_schema(table, {}).fieldNames()
        assert meta["cursor_field"] in names
        assert all(pk in names for pk in meta["primary_keys"])


def test_invalid_table_raises(connector):
    with pytest.raises(ValueError):
        connector.get_table_schema("nope", {})
    with pytest.raises(ValueError):
        connector.read_table_metadata("nope", {})
    with pytest.raises(ValueError):
        connector.read_table("nope", {}, {})


def test_read_table_converges_when_caught_up(connector):
    """A future start_date leaves no window to process; the offset is stable,
    so Trigger.AvailableNow terminates immediately (no network)."""
    opts = {"start_date": "2999-01-01"}
    records, offset = connector.read_table("cases", {}, opts)
    assert list(records) == []
    records2, offset2 = connector.read_table("cases", offset, opts)
    assert list(records2) == []
    assert offset2 == offset  # equality => converged
